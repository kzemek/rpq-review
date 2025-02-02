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
//! still maintaining data in memory. This basically means that a object can be added to the queue and then removed
//! without the disk commit ever blocking the processes sending or reciving the data. In the case that a batch of data
//! has already been removed from the queue before it is written to disk, the data is simply discarded. This
//! dramaically reduces the amount of time spent doing disk commits and allows for much better performance in the
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
/// You don't need to interact with the items in this struct directly,
/// but instead via the implementations attched to the RPQ struct.
pub struct RPQ<T: Ord + Clone + Send> {
    // options is the configuration for the RPQ
    options: RPQOptions,
    // non_empty_buckets is a binary heap of priorities
    non_empty_buckets: bpq::BucketPriorityQueue,
    // buckets is a map of priorities to a binary heap of items
    buckets: Arc<HashMap<usize, pq::PriorityQueue<T>>>,

    // items_in_queues is the number of items across all queues
    items_in_queues: AtomicUsize,
    // disk_cache maintains a cache of items that are in the queue
    disk_cache: Option<Arc<Database>>,
    // lazy_disk_channel is the channel for lazy disk writes
    lazy_disk_writer_sender: Arc<Sender<pq::Item<T>>>,
    // lazy_disk_reader is the receiver for lazy disk writes
    lazy_disk_writer_receiver: Mutex<Receiver<pq::Item<T>>>,
    // lazy_disk_delete_sender is the sender for lazy disk deletes
    lazy_disk_delete_sender: Arc<Sender<pq::Item<T>>>,
    // lazy_disk_delete_receiver is the receiver for lazy disk deletes
    lazy_disk_delete_receiver: Mutex<Receiver<pq::Item<T>>>,

    // batch_handler is the handler for batches
    batch_handler: Mutex<BatchHandler>,
    // batch_counter is the counter for batches
    batch_counter: Mutex<BatchCounter>,
    // batch_shutdown_receiver is the receiver for the shutdown signal
    batch_shutdown_receiver: watch::Receiver<bool>,
    // batch_shutdown_sender is the sender for the shutdown signal
    batch_shutdown_sender: watch::Sender<bool>,

    // shutdown_receiver is the receiver for the shutdown signal
    shutdown_receiver: watch::Receiver<bool>,
    // shutdown_sender is the sender for the shutdown signal
    shutdown_sender: watch::Sender<bool>,
    // sync_handles is a map of priorities to sync handles
    sync_handles: Mutex<Vec<JoinHandle<()>>>,
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

impl<T: Ord + Clone + Send + Sync> RPQ<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    /// Creates a new RPQ with the given options and returns the RPQ and the number of items restored from the disk cache
    pub async fn new(options: RPQOptions) -> Result<(Arc<RPQ<T>>, usize), Box<dyn Error>> {
        // Create base structures
        let mut buckets = HashMap::new();
        let items_in_queues = AtomicUsize::new(0);
        let sync_handles = Vec::new();
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

        // Create the lazy disk sync channel
        let (lazy_disk_writer_sender, lazy_disk_writer_receiver) =
            channel(options.buffer_size as usize);
        let (lazy_disk_delete_sender, lazy_disk_delete_receiver) =
            channel(options.buffer_size as usize);

        // Capture some variables
        let path = options.database_path.clone();
        let disk_cache_enabled = options.disk_cache_enabled;
        let lazy_disk_cache = options.lazy_disk_cache;

        // Create the buckets
        for i in 0..options.max_priority {
            buckets.insert(i, pq::PriorityQueue::new());
        }

        let disk_cache: Option<Arc<Database>>;
        if disk_cache_enabled {
            let db = Database::create(&path).unwrap();
            let db = Arc::new(db);
            disk_cache = Some(db);
        } else {
            disk_cache = None;
        }

        // Create the RPQ
        let rpq = RPQ {
            options,
            non_empty_buckets: bpq::BucketPriorityQueue::new(),
            buckets: Arc::new(buckets),
            items_in_queues,
            disk_cache,
            lazy_disk_writer_sender: Arc::new(lazy_disk_writer_sender),
            lazy_disk_writer_receiver: Mutex::new(lazy_disk_writer_receiver),
            lazy_disk_delete_sender: Arc::new(lazy_disk_delete_sender),
            lazy_disk_delete_receiver: Mutex::new(lazy_disk_delete_receiver),
            sync_handles: Mutex::new(sync_handles),
            shutdown_receiver,
            shutdown_sender,
            batch_handler: Mutex::new(batch_handler),
            batch_shutdown_sender: batch_shutdown_sender,
            batch_shutdown_receiver: batch_shutdown_receiver,
            batch_counter: Mutex::new(batch_counter),
        };
        let rpq = Arc::new(rpq);

        // Restore the items from the disk cache
        let mut restored_items: usize = 0;
        if disk_cache_enabled {
            // Create a the initial table
            let ctxn = rpq.disk_cache.as_ref().unwrap().begin_write().unwrap();
            ctxn.open_table(DB).unwrap();
            ctxn.commit().unwrap();

            let read_txn = rpq.disk_cache.as_ref().unwrap().begin_read().unwrap();
            let table = read_txn.open_table(DB).unwrap();

            let cursor = match table.range::<&str>(..) {
                Ok(range) => range,
                Err(e) => {
                    return Err(Box::<dyn Error>::from(e));
                }
            };

            for (_i, entry) in cursor.enumerate() {
                match entry {
                    Ok((_key, value)) => {
                        let item = pq::Item::from_bytes(value.value());

                        if item.is_err() {
                            return Err(Box::<dyn Error>::from(IoError::new(
                                ErrorKind::InvalidInput,
                                "Error reading from disk cache",
                            )));
                        }

                        // Mark the item as restored
                        let mut i = item.unwrap();
                        i.set_restored();
                        let result = rpq.enqueue(i).await;
                        if result.is_err() {
                            return Err(Box::<dyn Error>::from(IoError::new(
                                ErrorKind::InvalidInput,
                                "Error enqueueing item from the disk cache",
                            )));
                        }
                        restored_items += 1;
                    }
                    Err(e) => {
                        return Err(Box::<dyn Error>::from(e));
                    }
                }
            }
            _ = read_txn.close();

            if lazy_disk_cache {
                let mut handles = rpq.sync_handles.lock().await;
                let rpq_clone = Arc::clone(&rpq);
                handles.push(tokio::spawn(async move {
                    let result = rpq_clone.lazy_disk_writer().await;
                    if result.is_err() {
                        println!("Error in lazy disk writer: {:?}", result.err().unwrap());
                    }
                }));

                let rpq_clone = Arc::clone(&rpq);
                handles.push(tokio::spawn(async move {
                    let result = rpq_clone.lazy_disk_deleter().await;
                    if result.is_err() {
                        println!("Error in lazy disk deleter: {:?}", result.err().unwrap());
                    }
                }));
            }
        }
        Ok((rpq, restored_items))
    }

    /// Adds an item to the RPQ and returns an error if one occurs otherwise it returns ()
    pub async fn enqueue(&self, mut item: pq::Item<T>) -> Result<(), Box<dyn Error>> {
        // Check if the item priority is greater than the bucket count
        if item.priority >= self.options.max_priority {
            return Result::Err(Box::<dyn Error>::from(IoError::new(
                ErrorKind::InvalidInput,
                "Priority is greater than bucket count",
            )));
        }
        let priority = item.priority;

        // Get the bucket and enqueue the item
        let bucket = self.buckets.get(&item.priority);

        if bucket.is_none() {
            return Result::Err(Box::<dyn Error>::from(IoError::new(
                ErrorKind::InvalidInput,
                "Bucket does not exist",
            )));
        }

        // If the disk cache is enabled, send the item to the lazy disk writer
        if self.options.disk_cache_enabled {
            // Increment the batch number
            let mut batch_counter = self.batch_counter.lock().await;
            batch_counter.message_counter += 1;
            if batch_counter.message_counter % self.options.lazy_disk_cache_batch_size == 0 {
                batch_counter.batch_number += 1;
            }
            let bn = batch_counter.batch_number;
            drop(batch_counter);

            item.set_batch_id(bn);
            if !item.was_restored() {
                item.set_disk_uuid();
                if self.options.lazy_disk_cache {
                    let lazy_disk_writer_sender = &self.lazy_disk_writer_sender;
                    let was_sent = lazy_disk_writer_sender.send(item.clone()).await;
                    match was_sent {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(Box::<dyn Error>::from(e));
                        }
                    }
                } else {
                    let result = self.commit_single(item.clone());
                    match result {
                        Ok(_) => {}
                        Err(e) => {
                            return Result::Err(e);
                        }
                    }
                }
            }
        }

        // Enqueue the item and update
        bucket.unwrap().enqueue(item);
        self.non_empty_buckets.add_bucket(priority);
        Ok(())
    }

    /// Returns a Result with the next item in the RPQ or an error if one occurs
    pub async fn dequeue(&self) -> Result<Option<pq::Item<T>>, Box<dyn Error>> {
        // Fetch the bucket
        let bucket_id = self.non_empty_buckets.peek();
        if bucket_id.is_none() {
            return Result::Err(Box::<dyn Error>::from(IoError::new(
                ErrorKind::InvalidInput,
                "No items in queue",
            )));
        }
        let bucket_id = bucket_id.unwrap();

        // Fetch the queue
        let queue = self.buckets.get(&bucket_id);
        if queue.is_none() {
            return Result::Err(Box::<dyn Error>::from(IoError::new(
                ErrorKind::InvalidInput,
                "No items in queue",
            )));
        }

        // Fetch the item from the bucket
        let item = queue.unwrap().dequeue();
        if item.is_none() {
            return Result::Err(Box::<dyn Error>::from(IoError::new(
                ErrorKind::InvalidInput,
                "No items in queue",
            )));
        }
        self.items_in_queues.fetch_sub(1, Ordering::SeqCst);
        let item = item.unwrap();

        // If the bucket is empty, remove it from the non_empty_buckets
        if queue.unwrap().len() == 0 {
            self.non_empty_buckets.remove_bucket(&bucket_id);
        }

        if self.options.disk_cache_enabled {
            let item_clone = item.clone();
            if self.options.lazy_disk_cache {
                let lazy_disk_delete_sender = &self.lazy_disk_delete_sender;
                let was_sent = lazy_disk_delete_sender.send(item_clone).await;
                match was_sent {
                    Ok(_) => {}
                    Err(e) => {
                        return Result::Err(Box::new(e));
                    }
                }
            } else {
                let result = self.delete_single(item_clone.get_disk_uuid().unwrap().as_ref());
                if result.is_err() {
                    return Result::Err(result.err().unwrap());
                }
            }
        }

        Ok(Some(item))
    }

    /// Prioritize reorders the items in each bucket based on the values spesified in the item.
    /// It returns a tuple with the number of items removed and the number of items escalated or and error if one occurs.
    pub async fn prioritize(&self) -> Result<(usize, usize), Box<dyn Error>> {
        let mut removed: usize = 0;
        let mut escalated: usize = 0;

        for (_, active_bucket) in self.buckets.iter() {
            match active_bucket.prioritize() {
                Ok((r, e)) => {
                    removed += r;
                    escalated += e;
                }
                Err(err) => {
                    return Err(Box::<dyn Error>::from(err));
                }
            }
        }
        self.items_in_queues.fetch_sub(removed, Ordering::SeqCst);
        Ok((removed, escalated))
    }

    async fn lazy_disk_writer(&self) -> Result<(), Box<dyn Error>> {
        let mut awaiting_batches = HashMap::<usize, Vec<pq::Item<T>>>::new();
        let mut ticker = interval(self.options.lazy_disk_write_delay);
        let mut receiver = self.lazy_disk_writer_receiver.lock().await;
        let mut shutdown_receiver = self.shutdown_receiver.clone();

        loop {
            // Check if the write cache is full or the ticker has ticked
            tokio::select! {
                // Flush the cache if the ticker has ticked
                _ = ticker.tick() => {
                    let mut batch_handler = self.batch_handler.lock().await;
                    for (id, batch) in awaiting_batches.iter_mut() {

                        if batch.len() >= self.options.lazy_disk_cache_batch_size {
                            if *batch_handler.deleted_batches.get(id).unwrap_or(&false) {
                                batch.clear();
                            } else {
                                let result = self.commit_batch(batch);
                                if result.is_err() {
                                    return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                }
                            }
                            batch_handler.synced_batches.insert(*id, true);
                            batch_handler.deleted_batches.insert(*id, false);
                        }
                    }
                },

                // Add the item to the cache if it is received
                item = receiver.recv() => {
                    if let Some(item) = item {
                        let batch_bucket = item.get_batch_id();
                        let batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);
                    }
                },

                // Shutdown the writer if the shutdown signal is received
                _ = shutdown_receiver.changed() => {
                    receiver.close();

                    // Pull the remaining items from the receiver
                    while let Some(item) = receiver.recv().await {
                        let batch_bucket = item.get_batch_id();
                        let batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);
                    }

                    // Commit the remaining batches
                    for (id, batch) in awaiting_batches.iter_mut() {
                        let mut batch_handler = self.batch_handler.lock().await;

                        if *batch_handler.deleted_batches.get(id).unwrap_or(&false) {
                            batch.clear();
                            continue;
                        }

                        batch_handler.synced_batches.insert(*id, true);
                        batch_handler.deleted_batches.insert(*id, false);
                        let result = self.commit_batch(batch);
                        if result.is_err() {
                            return Err(Box::<dyn Error>::from(result.err().unwrap()));
                        }
                    }
                    self.batch_shutdown_sender.send(true).unwrap();

                    break Ok(());
                }
            }
        }
    }

    async fn lazy_disk_deleter(&self) -> Result<(), Box<dyn Error>> {
        let mut awaiting_batches = HashMap::<usize, Vec<pq::Item<T>>>::new();
        let mut restored_items: Vec<pq::Item<T>> = Vec::new();
        let mut receiver = self.lazy_disk_delete_receiver.lock().await;
        let mut shutdown_receiver = self.batch_shutdown_receiver.clone();

        loop {
            // Check if the write cache is full or the ticker has ticked
            tokio::select! {
                item = receiver.recv() => {
                    // Check if the item was restored
                    if let Some(item) = item {
                        if item.was_restored() {
                            restored_items.push(item);

                            if restored_items.len() >= self.options.lazy_disk_cache_batch_size {
                                let result = self.delete_batch(&mut restored_items);
                                if result.is_err() {
                                    return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                }
                                restored_items.clear();
                            }
                            continue;
                        }

                        // If the item was not restored, add it to the batch
                        let batch_bucket = item.get_batch_id();
                        let batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);

                        // Check if the batch is full
                        if batch.len() >= self.options.lazy_disk_cache_batch_size {
                            let mut batch_handler = self.batch_handler.lock().await;
                            let was_synced = batch_handler.synced_batches.get(&batch_bucket).unwrap_or(&false);
                            if *was_synced {
                                let result = self.delete_batch(batch);
                                if result.is_err() {
                                    return Err(Box::<dyn Error>::from(result.err().unwrap()));
                                }
                                awaiting_batches.remove(&batch_bucket);
                            } else {
                                batch.clear();
                                awaiting_batches.remove(&batch_bucket);
                            }

                            batch_handler.deleted_batches.insert(batch_bucket, true);
                            batch_handler.synced_batches.insert(batch_bucket, false);
                        }
                    }
                },
                // Shutdown the writer if the shutdown signal is received
                _ = shutdown_receiver.changed() => {
                    receiver.close();

                    // Pull the remaining items from the receiver
                    while let Some(item) = receiver.recv().await {
                        // Check if the item was restored
                        if item.was_restored() {
                            restored_items.push(item);
                            continue;
                        }

                        // If the item was not restored, add it to the batch
                        let batch_bucket = item.get_batch_id();
                        let batch = awaiting_batches.entry(batch_bucket).or_insert(Vec::new());
                        batch.push(item);
                    }

                    // Commit the remaining batches
                    if !restored_items.is_empty() {
                        let result = self.delete_batch(&mut restored_items);
                        if result.is_err() {
                            return Err(Box::<dyn Error>::from(result.err().unwrap()));
                        }
                        restored_items.clear();
                    }
                    for (id, batch) in awaiting_batches.iter_mut() {
                        let mut batch_handler = self.batch_handler.lock().await;
                        let was_synced = batch_handler.synced_batches.get(id).unwrap_or(&false);
                        if *was_synced {
                            let result = self.delete_batch(batch);
                            if result.is_err() {
                                return Err(Box::<dyn Error>::from(result.err().unwrap()));
                            }
                        } else {
                            batch.clear();
                        }
                        batch_handler.deleted_batches.insert(*id, true);
                        batch_handler.synced_batches.insert(*id, false);
                    }

                    break Ok(());
                }
            }
        }
    }

    fn commit_batch(&self, write_cache: &mut Vec<pq::Item<T>>) -> Result<(), Box<dyn Error>> {
        let write_txn = self.disk_cache.as_ref().unwrap().begin_write().unwrap();
        for item in write_cache.iter() {
            let mut table = write_txn.open_table(DB).unwrap();
            // Convert to bytes
            let b = item.to_bytes();
            if b.is_err() {
                return Err(Box::<dyn Error>::from(IoError::new(
                    ErrorKind::InvalidInput,
                    "Error converting item to bytes",
                )));
            }

            let b = b.unwrap();
            let key = item.get_disk_uuid().unwrap();

            let was_written = table.insert(key.as_str(), &b[..]);
            if was_written.is_err() {
                return Err(Box::<dyn Error>::from(IoError::new(
                    ErrorKind::InvalidInput,
                    "Error writing item to disk cache",
                )));
            }
        }

        write_txn.commit().unwrap();
        write_cache.clear();
        Ok(())
    }

    fn delete_batch(&self, delete_cache: &mut Vec<pq::Item<T>>) -> Result<(), Box<dyn Error>> {
        let write_txn = self.disk_cache.as_ref().unwrap().begin_write().unwrap();
        for item in delete_cache.iter() {
            let mut table = write_txn.open_table(DB).unwrap();
            let key = item.get_disk_uuid().unwrap();
            let was_deleted = table.remove(key.as_str());
            if was_deleted.is_err() {
                return Err(Box::<dyn Error>::from(IoError::new(
                    ErrorKind::InvalidInput,
                    "Error deleting item from disk cache",
                )));
            }
        }

        write_txn.commit().unwrap();
        delete_cache.clear();
        Ok(())
    }

    fn commit_single(&self, item: pq::Item<T>) -> Result<(), Box<dyn Error>> {
        let write_txn = self.disk_cache.as_ref().unwrap().begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DB).unwrap();
            let b = item.to_bytes();

            if b.is_err() {
                return Err(Box::<dyn Error>::from(IoError::new(
                    ErrorKind::InvalidInput,
                    "Error converting item to bytes",
                )));
            }

            let key = item.get_disk_uuid().unwrap();
            let b = b.unwrap();

            let was_written = table.insert(key.as_str(), &b[..]);
            if was_written.is_err() {
                return Err(Box::<dyn Error>::from(IoError::new(
                    ErrorKind::InvalidInput,
                    "Error writing item to disk cache",
                )));
            }
        }

        write_txn.commit().unwrap();
        Ok(())
    }

    fn delete_single(&self, key: &str) -> Result<(), Box<dyn Error>> {
        let write_txn = self.disk_cache.as_ref().unwrap().begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DB).unwrap();
            let was_written = table.remove(key);
            if was_written.is_err() {
                return Err(Box::<dyn Error>::from(IoError::new(
                    ErrorKind::InvalidInput,
                    "Error deleting item from disk cache",
                )));
            }
        }

        write_txn.commit().unwrap();
        Ok(())
    }

    /// Returns the number of items in the RPQ across all buckets
    pub async fn len(&self) -> usize {
        let mut len = 0 as usize;
        for (_, active_bucket) in self.buckets.iter() {
            len += active_bucket.len();
        }
        len
    }

    /// Returns the number of active buckets in the RPQ (buckets with items)
    pub fn active_buckets(&self) -> usize {
        self.non_empty_buckets.len()
    }

    /// Returns the number of pending batches in the RPQ for both the writer or the deleter
    pub async fn unsynced_batches(&self) -> usize {
        let mut unsynced_batches = 0;
        let batch_handler = self.batch_handler.lock().await;
        for (_, synced) in batch_handler.synced_batches.iter() {
            if !*synced {
                unsynced_batches += 1;
            }
        }
        for (_, deleted) in batch_handler.deleted_batches.iter() {
            if !*deleted {
                unsynced_batches += 1;
            }
        }
        unsynced_batches
    }

    /// Returns the number of items in the disk cache which can be helpful for debugging or monitoring
    pub fn items_in_db(&self) -> usize {
        if self.disk_cache.is_none() {
            return 0;
        }
        let read_txn = self.disk_cache.as_ref().unwrap().begin_read().unwrap();
        let table = read_txn.open_table(DB).unwrap();
        let count = table.len().unwrap();
        count as usize
    }

    /// Closes the RPQ and waits for all the async tasks to finish
    pub async fn close(&self) {
        self.shutdown_sender.send(true).unwrap();

        let mut handles = self.sync_handles.lock().await;
        while let Some(handle) = handles.pop() {
            handle.await.unwrap();
        }
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

        let r: Result<(Arc<RPQ<usize>>, usize), Box<dyn Error>> = RPQ::new(options).await;
        if r.is_err() {
            panic!("Error creating RPQ");
        }
        let (rpq, _restored_items) = r.unwrap();

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
            let result = rpq.enqueue(item).await;
            if result.is_err() {
                panic!("Error enqueueing item");
            }
            let v = expected_data.entry(i % 10).or_insert(VecDeque::new());
            v.push_back(i);
        }

        for _i in 0..message_count {
            let item = rpq.dequeue().await;
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
        let r = RPQ::new(options).await;
        if r.is_err() {
            panic!("Error creating RPQ");
        }
        let (rpq, restored_items) = r.unwrap();

        // Launch the monitoring thread
        let rpq_clone = Arc::clone(&rpq);
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
                        let results = rpq_clone.prioritize().await;

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
            let rpq_clone = Arc::clone(&rpq);
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

                    let result = rpq_clone.enqueue(item).await;
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
            let rpq_clone = Arc::clone(&rpq);
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

                    let item = rpq_clone.dequeue().await;
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
        rpq.close().await;

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
