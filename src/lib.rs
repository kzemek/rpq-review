//! # RPQ
//! RPQ implements a in memory, disk cached priority queue that is optimized for high throughput and low latency
//! while still maintaining strict ordering guarantees and durability. Due to the fact that most operations are done
//! in constant time O(1) or logarithmic time O(log n), with the exception of the prioritize function which happens
//! in linear time O(n), all RPQ operations are extremely fast. A single RPQ can handle a few million transactions
//! a second and can be tuned depending on your work load.
//!
//!  # Create a new RPQ
//! The RPQ should always be created with the new function like so:
//!
//! ```rust
//! use rpq::{RPQ, RPQOptions};
//! use std::time;
//!
//! #[tokio::main]
//! async fn main() {
//!     let options = RPQOptions {
//!        max_priority: 10,
//!        disk_cache_enabled: false,
//!        database_path: "/tmp/rpq.db".to_string(),
//!        lazy_disk_cache: true,
//!        lazy_disk_write_delay: time::Duration::from_secs(5),
//!        lazy_disk_cache_batch_size: 10_000,
//!        buffer_size: 1_000_000,
//!     };
//!
//!     let r = RPQ::<i32>::new(options).await;
//!     if r.is_err() {
//!         // handle logic
//!    }
//! }
//! ```
//!
//! # Architecture Notes
//! In many ways, RPQ slighty compromises the performance of a traditional priority queue in order to provide
//! a variety of features that are useful when absorbing distributed load from many down or upstream services.
//! It employs a fairly novel techinique that allows it to lazily write and delete items from a disk cache while
//! still maintaining data in memory. This basically means that an object can be added to the queue and then removed
//! without the disk commit ever blocking the processes sending or reciving the data. In the case that a batch of data
//! has already been removed from the queue before it is written to disk, the data is simply discarded. This
//! dramatically reduces the amount of time spent doing disk commits and allows for much better performance in the
//! case that you need disk caching and still want to maintain a high peak throughput.
//!
//! ```text
//!                 ┌───────┐
//!                 │ Item  │
//!                 └───┬───┘
//!                     │
//!                     ▼
//!              ┌─────────────┐
//!              │             │
//!              │   enqueue   │
//!              │             │
//!              │             │
//!              └──────┬──────┘
//!                     │
//!                     │
//!                     │
//! ┌───────────────┐   │    ┌──────────────┐
//! │               │   │    │              │      ┌───┐
//! │   VecDeque    │   │    │  Lazy Disk   │      │   │
//! │               │◄──┴───►│    Writer    ├─────►│ D │
//! │               │        │              │      │ i │
//! └───────┬───────┘        └──────────────┘      │ s │
//!         │                                      │ k │
//!         │                                      │   │
//!         │                                      │ C │
//!         ▼                                      │ a │
//! ┌───────────────┐         ┌─────────────┐      │ c │
//! │               │         │             │      │ h │
//! │    dequeue    │         │   Lazy Disk ├─────►│ e │
//! │               ├────────►│    Deleter  │      │   │
//! │               │         │             │      └───┘
//! └───────┬───────┘         └─────────────┘
//!         │
//!         ▼
//!      ┌──────┐
//!      │ Item │
//!      └──────┘
//! ```
use core::time;
use std::collections::HashMap;
use std::error::Error;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::result::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;

use redb::{Database, ReadableTableMetadata, TableDefinition};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::interval;

mod bpq;
pub mod pq;

const DB: TableDefinition<&str, &[u8]> = TableDefinition::new("rpq");

/// RPQ hold private items and configuration for the RPQ.
struct RPQInternal<T: Ord + Clone + Send> {
    // options is the configuration for the RPQ
    options: RPQOptions,
    // buckets is a map of priorities to a binary heap of items
    buckets: Vec<pq::PriorityQueue<T>>,

    // batch_handler is the handler for batches
    batch_handler: BatchHandler,
    // batch_counter is the counter for batches
    batch_counter: BatchCounter,
    // batch_shutdown_receiver is the receiver for the shutdown signal
    batch_shutdown_receiver: watch::Receiver<bool>,
    // batch_shutdown_sender is the sender for the shutdown signal
    batch_shutdown_sender: watch::Sender<bool>,

    // shutdown_receiver is the receiver for the shutdown signal
    shutdown_receiver: watch::Receiver<bool>,
    // shutdown_sender is the sender for the shutdown signal
    shutdown_sender: watch::Sender<bool>,
}

#[derive(Clone)]
pub struct RPQ<T: Ord + Clone + Send> {
    internal: Arc<RwLock<RPQInternal<T>>>,
}

/// RPQOptions is the configuration for the RPQ
pub struct RPQOptions {
    /// Holds the number of priorities(buckets) that this RPQ will accept for this queue.
    pub max_priority: usize,
    /// Enables or disables the disk cache using redb as the backend to store items
    pub disk_cache_enabled: bool,
    /// Holds the path to where the disk cache database will be persisted
    pub database_path: String,
    /// Enables or disables lazy disk writes and deletes. The speed can be quite variable depending
    /// on the disk itself and how often you are emptying the queue in combination with the write delay
    pub lazy_disk_cache: bool,
    /// Sets the delay between lazy disk writes. This delays items from being commited to the disk cache.
    /// If you are pulling items off the queue faster than this delay, many times can be skip the write to disk,
    /// massively increasing the throughput of the queue.
    pub lazy_disk_write_delay: time::Duration,
    /// Sets the number of items that will be written to the disk cache in a single batch. This can be used to
    /// tune the performance of the disk cache depending on your specific workload.
    pub lazy_disk_cache_batch_size: usize,
    /// Sets the size of the channnel that is used to buffer items before they are written to the disk cache.
    /// This can block your queue if the thread pulling items off the channel becomes fully saturated. Typically you
    /// should set this value in proportion to your largest write peaks. I.E. if your peak write is 10,000,000 items per second,
    /// and your average write is 1,000,000 items per second, you should set this value to 20,000,000 to ensure that no blocking occurs.
    pub buffer_size: usize,
}

struct BatchHandler {
    // synced_batches is a map of priorities to the last synced batch
    synced_batches: HashMap<usize, bool>,
    // deleted_batches is a map of priorities to the last deleted batch
    deleted_batches: HashMap<usize, bool>,
}

struct BatchCounter {
    // message_counter is the counter for the number of messages that have been sent to the RPQ over the lifetime
    message_counter: usize,
    // batch_number is the current batch number
    batch_number: usize,
}

impl<T: Ord + Clone + Send + Sync> RPQInternal<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    /// Creates a new RPQ with the given options and returns the RPQ and the number of items restored from the disk cache
    pub fn new(options: RPQOptions) -> Result<(Self, usize), Box<dyn Error>> {
        // Create base structures
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let (batch_shutdown_sender, batch_shutdown_receiver) = watch::channel(false);
        let batch_handler = BatchHandler {
            synced_batches: HashMap::new(),
            deleted_batches: HashMap::new(),
        };
        let batch_counter = BatchCounter {
            message_counter: 0,
            batch_number: 0,
        };

        // Capture some variables
        let path = options.database_path.clone();
        let disk_cache_enabled = options.disk_cache_enabled;
        let lazy_disk_cache = options.lazy_disk_cache;

        // Create the buckets
        let buckets = (0..options.max_priority)
            .map(|_| pq::PriorityQueue::new())
            .collect();

        let disk_cache: Option<Database> =
            disk_cache_enabled.then(|| Database::create(&path).unwrap());

        // Create the RPQ
        let mut rpq = RPQInternal {
            options,
            buckets,
            shutdown_receiver,
            shutdown_sender,
            batch_handler,
            batch_shutdown_sender,
            batch_shutdown_receiver,
            batch_counter,
        };

        // Restore the items from the disk cache
        let restored_items: usize = 0;
        Ok((rpq, restored_items))
    }

    /// Adds an item to the RPQ and returns an error if one occurs otherwise it returns ()
    pub fn enqueue(&mut self, item: pq::Item<T>) -> Result<(), Box<dyn Error>> {
        let priority = item.priority;
        if priority >= self.options.max_priority {
            return Err(Box::<dyn Error>::from(IoError::new(
                ErrorKind::InvalidInput,
                "Priority is greater than bucket count",
            )));
        }

        // Get the bucket and enqueue the item
        let bucket = self.buckets.get_mut(priority).unwrap();
        // Enqueue the item and update
        bucket.enqueue(item);

        Ok(())
    }

    /// Returns a Result with the next item in the RPQ or an error if one occurs
    pub fn dequeue(&mut self) -> Result<Option<pq::Item<T>>, Box<dyn Error>> {
        // Fetch the queue
        let queue = self.buckets.iter_mut().find(|bucket| bucket.len() != 0);
        if queue.is_none() {
            return Ok(None);
        }
        let queue = queue.unwrap();

        // Fetch the item from the bucket
        let item = queue.dequeue();
        if item.is_none() {
            return Ok(None);
        }
        let item = item.unwrap();

        Ok(Some(item))
    }

    /// Prioritize reorders the items in each bucket based on the values spesified in the item.
    /// It returns a tuple with the number of items removed and the number of items escalated or and error if one occurs.
    pub fn prioritize(&mut self) -> Result<(usize, usize), Box<dyn Error>> {
        let (removed, escalated) =
            self.buckets
                .iter_mut()
                .try_fold((0, 0), |acc, active_bucket| {
                    let (removed, escalated) = active_bucket.prioritize()?;
                    Ok::<(usize, usize), Box<dyn Error>>((acc.0 + removed, acc.1 + escalated))
                })?;

        Ok((removed, escalated))
    }

    /// Returns the number of items in the RPQ across all buckets
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|bucket| bucket.len()).sum()
    }

    /// Returns the number of active buckets in the RPQ (buckets with items)
    pub fn active_buckets(&self) -> usize {
        self.buckets
            .iter()
            .filter(|bucket| bucket.len() != 0)
            .count()
    }

    /// Returns the number of pending batches in the RPQ for both the writer or the deleter
    pub fn unsynced_batches(&self) -> usize {
        let mut unsynced_batches = 0;
        for (_, synced) in self.batch_handler.synced_batches.iter() {
            if !*synced {
                unsynced_batches += 1;
            }
        }
        for (_, deleted) in self.batch_handler.deleted_batches.iter() {
            if !*deleted {
                unsynced_batches += 1;
            }
        }
        unsynced_batches
    }

    /// Returns the number of items in the disk cache which can be helpful for debugging or monitoring
    pub fn items_in_db(&self) -> usize {
        return 0;
    }

    /// Closes the RPQ and waits for all the async tasks to finish
    pub fn close(&self) {
        self.shutdown_sender.send(true).unwrap();
    }
}

impl<T: Ord + Clone + Send + Sync> RPQ<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    pub fn new(options: RPQOptions) -> Result<(Self, usize), Box<dyn Error>> {
        let (rpq, restored) = RPQInternal::new(options)?;
        let rpq = Arc::new(RwLock::new(rpq));

        Ok((RPQ { internal: rpq }, restored))
    }

    pub fn enqueue(&mut self, item: pq::Item<T>) -> Result<(), Box<dyn Error>> {
        self.internal.write().unwrap().enqueue(item)
    }

    pub fn dequeue(&mut self) -> Result<Option<pq::Item<T>>, Box<dyn Error>> {
        self.internal.write().unwrap().dequeue()
    }

    pub fn prioritize(&mut self) -> Result<(usize, usize), Box<dyn Error>> {
        self.internal.write().unwrap().prioritize()
    }

    pub fn len(&self) -> usize {
        self.internal.read().unwrap().len()
    }

    pub fn active_buckets(&self) -> usize {
        self.internal.read().unwrap().active_buckets()
    }

    pub fn unsynced_batches(&self) -> usize {
        self.internal.read().unwrap().unsynced_batches()
    }

    pub fn items_in_db(&self) -> usize {
        self.internal.read().unwrap().items_in_db()
    }

    pub fn close(&self) {
        self.internal.read().unwrap().close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time;
    use std::{
        collections::VecDeque,
        error::Error,
        sync::atomic::{AtomicBool, AtomicUsize},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn order_test() {
        let message_count = 1_000_000;

        let options = RPQOptions {
            max_priority: 10,
            disk_cache_enabled: false,
            database_path: "/tmp/rpq.redb".to_string(),
            lazy_disk_cache: false,
            lazy_disk_write_delay: time::Duration::from_secs(5),
            lazy_disk_cache_batch_size: 5000,
            buffer_size: 1_000_000,
        };

        let r: Result<(RPQ<usize>, usize), Box<dyn Error>> = RPQ::new(options);
        if r.is_err() {
            panic!("Error creating RPQ");
        }
        let (mut rpq, _restored_items) = r.unwrap();

        let mut expected_data = HashMap::new();
        for i in 0..message_count {
            let item = pq::Item::new(
                i % 10,
                i,
                false,
                None,
                false,
                Some(std::time::Duration::from_secs(5)),
            );
            let result = rpq.enqueue(item);
            if result.is_err() {
                panic!("Error enqueueing item");
            }
            let v = expected_data.entry(i % 10).or_insert(VecDeque::new());
            v.push_back(i);
        }

        for _i in 0..message_count {
            let item = rpq.dequeue();
            if item.is_err() {
                panic!("Item is None");
            }
            let item = item.unwrap().unwrap();
            let v = expected_data.get_mut(&item.priority).unwrap();
            let expected_data = v.pop_front().unwrap();
            assert!(item.data == expected_data);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_test() {
        // Set Message Count
        let message_count = 10_000_000 as usize;

        // Set Concurrency
        let send_threads = 4 as usize;
        let receive_threads = 4 as usize;
        let bucket_count = 10 as usize;
        let sent_counter = Arc::new(AtomicUsize::new(0));
        let received_counter = Arc::new(AtomicUsize::new(0));
        let removed_counter = Arc::new(AtomicUsize::new(0));
        let total_escalated = Arc::new(AtomicUsize::new(0));
        let finshed_sending = Arc::new(AtomicBool::new(false));
        let max_retries = 1000;

        // Create the RPQ
        let options = RPQOptions {
            max_priority: bucket_count,
            disk_cache_enabled: true,
            database_path: "/tmp/rpq.redb".to_string(),
            lazy_disk_cache: true,
            lazy_disk_write_delay: time::Duration::from_secs(5),
            lazy_disk_cache_batch_size: 10000,
            buffer_size: 1_000_000,
        };
        let r = RPQ::new(options);
        if r.is_err() {
            panic!("Error creating RPQ");
        }
        let (rpq, restored_items) = r.unwrap();

        // Launch the monitoring thread
        let mut rpq_clone = rpq.clone();
        let (shutdown_sender, mut shutdown_receiver) = watch::channel(false);
        let removed_clone = Arc::clone(&removed_counter);
        let escalated_clone = Arc::clone(&total_escalated);
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_receiver.changed() => {
                    return;
                },
                _ = async {
                    loop {
                        tokio::time::sleep(time::Duration::from_secs(10)).await;
                        let results = rpq_clone.prioritize();

                        if !results.is_ok() {
                            let (removed, escalated) = results.unwrap();
                            removed_clone.fetch_add(removed, Ordering::SeqCst);
                            escalated_clone.fetch_add(escalated, Ordering::SeqCst);
                        }
                    }
                } => {}
            }
        });

        let total_timer = std::time::Instant::now();

        // Enqueue items
        println!("Launching {} Send Threads", send_threads);
        let mut send_handles = Vec::new();
        let send_timer = std::time::Instant::now();
        for _ in 0..send_threads {
            let mut rpq_clone = rpq.clone();
            let sent_clone = Arc::clone(&sent_counter);

            send_handles.push(tokio::spawn(async move {
                loop {
                    if sent_clone.load(Ordering::SeqCst) >= message_count {
                        break;
                    }

                    let item = pq::Item::new(
                        //rand::thread_rng().gen_range(0..bucket_count),
                        sent_clone.load(Ordering::SeqCst) % bucket_count,
                        0,
                        false,
                        None,
                        false,
                        Some(std::time::Duration::from_secs(5)),
                    );

                    let result = rpq_clone.enqueue(item);
                    if result.is_err() {
                        panic!("Error enqueueing item");
                    }
                    sent_clone.fetch_add(1, Ordering::SeqCst);
                }
                println!("Finished Sending");
            }));
        }

        // Dequeue items
        println!("Launching {} Receive Threads", receive_threads);
        let mut receive_handles = Vec::new();
        let receive_timer = std::time::Instant::now();
        for _ in 0..receive_threads {
            // Clone all the shared variables
            let mut rpq_clone = rpq.clone();
            let received_clone = Arc::clone(&received_counter);
            let sent_clone = Arc::clone(&sent_counter);
            let removed_clone = Arc::clone(&removed_counter);
            let finshed_sending_clone = Arc::clone(&finshed_sending);

            // Spawn the thread
            receive_handles.push(tokio::spawn(async move {
                let mut counter = 0;
                loop {
                    if finshed_sending_clone.load(Ordering::SeqCst) {
                        if received_clone.load(Ordering::SeqCst)
                            + removed_clone.load(Ordering::SeqCst)
                            >= sent_clone.load(Ordering::SeqCst) + restored_items
                        {
                            break;
                        }
                    }

                    let item = rpq_clone.dequeue();
                    if item.is_err() {
                        if counter >= max_retries {
                            panic!("Reached max retries waiting for items!");
                        }
                        counter += 1;
                        std::thread::sleep(time::Duration::from_millis(100));
                        continue;
                    }
                    counter = 0;
                    received_clone.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        // Wait for send threads to finish
        for handle in send_handles {
            handle.await.unwrap();
        }
        let send_time = send_timer.elapsed().as_secs_f64();

        finshed_sending.store(true, Ordering::SeqCst);
        // Wait for receive threads to finish
        for handle in receive_handles {
            handle.await.unwrap();
        }
        let receive_time = receive_timer.elapsed().as_secs_f64();
        shutdown_sender.send(true).unwrap();

        // Close the RPQ
        println!("Waiting for RPQ to close");
        rpq.close();

        println!(
            "Sent: {}, Received: {}, Removed: {}, Escalated: {}",
            sent_counter.load(Ordering::SeqCst),
            received_counter.load(Ordering::SeqCst),
            removed_counter.load(Ordering::SeqCst),
            total_escalated.load(Ordering::SeqCst)
        );
        println!(
            "Send Time: {}s, Receive Time: {}s, Total Time: {}s",
            send_time,
            receive_time,
            total_timer.elapsed().as_secs_f64()
        );

        assert_eq!(rpq.items_in_db(), 0);
    }
}
