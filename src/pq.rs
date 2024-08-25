use std::time::Duration;
use std::{collections::VecDeque, sync::RwLock};

use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

mod ftime;

pub struct PriorityQueue<T: Ord + Clone + Send> {
    items: RwLock<VecDeque<Item<T>>>,
    ftime: ftime::CachedTime,
}

impl<T: Ord + Clone + Send> PriorityQueue<T> {
    pub fn new() -> PriorityQueue<T> {
        PriorityQueue {
            items: RwLock::new(VecDeque::new()),
            ftime: ftime::CachedTime::new(Duration::from_millis(250)),
        }
    }

    pub fn len(&self) -> u64 {
        self.items.read().unwrap().len() as u64
    }

    pub fn enqueue(&self, item: Item<T>) {
        let mut item = item;

        // Set the internal fields
        item.submitted_at = self.ftime.get_time().into();
        item.last_escalation = None;

        // Add the item to the queue
        self.items.write().unwrap().push_back(item);
    }

    pub fn dequeue(&self) -> Option<Item<T>> {
        self.items.write().unwrap().pop_front()
    }

    pub fn prioritize(&self) -> Option<(u64, u64)> {
        let mut items = self.items.write().unwrap();
        let mut to_remove = Vec::new();
        let mut to_swap = Vec::new();

        for (index, item) in items.iter_mut().enumerate() {
            // Timeout items that have been in the queue for too long
            if item.can_timeout {
                if item.timeout.unwrap().as_millis()
                    >= (self.ftime.get_time().timestamp_millis()
                        - item.submitted_at.unwrap().timestamp_millis())
                        as u128
                {
                    to_remove.push(index);
                    continue;
                }
            }

            // Escalate items that have been in the queue for too long
            if item.should_escalate {
                // Check if we have ever escalated this item
                if item.last_escalation.is_none() {
                    if item.escalation_rate.unwrap().as_millis()
                        > (self.ftime.get_time().timestamp_millis()
                            - item.submitted_at.unwrap().timestamp_millis())
                            as u128
                    {
                        item.last_escalation = Some(Utc::now());
                        if index > 0 {
                            to_swap.push(index);
                        }
                    }
                    continue;
                }

                // Check if we need to escalate this item again
                if item.escalation_rate.unwrap().as_millis()
                    >= (Utc::now().timestamp_millis()
                        - item.last_escalation.unwrap().timestamp_millis())
                        as u128
                {
                    item.last_escalation = Some(Utc::now());
                    if index > 0 {
                        to_swap.push(index);
                    }
                    continue;
                }
            }
        }

        let removed = to_remove.len();
        let swapped = to_swap.len();

        // Perform removals and swaps
        for index in to_remove.iter().rev() {
            items.remove(*index);
        }
        for index in to_swap {
            items.swap(index, index - 1);
        }

        return Some((removed as u64, swapped as u64));
    }
}

// Item is a struct that holds the data and metadata for an item in the queue
#[derive(Serialize, Deserialize, Clone)]
pub struct Item<T: Clone + Send> {
    // User
    pub priority: u64,
    pub data: T,
    pub should_escalate: bool,
    pub escalation_rate: Option<Duration>,
    pub can_timeout: bool,
    pub timeout: Option<Duration>,

    // Internal
    disk_uuid: Option<String>,
    submitted_at: Option<DateTime<Utc>>,
    last_escalation: Option<DateTime<Utc>>,
    batch_id: u64,
    was_restored: bool,
}

impl<T: Clone + Send> Item<T> {
    // Constructor to initialize the struct
    pub fn new(
        priority: u64,
        data: T,
        should_escalate: bool,
        escalation_rate: Option<Duration>,
        can_timeout: bool,
        timeout: Option<Duration>,
    ) -> Self {
        Item {
            // User-provided fields
            priority,
            data,
            should_escalate,
            escalation_rate,
            can_timeout,
            timeout,

            // Private with fn access
            batch_id: 0,
            was_restored: false,
            disk_uuid: None,

            // Internal fields
            submitted_at: None,
            last_escalation: None,
        }
    }

    pub fn set_disk_uuid(&mut self) {
        let id = uuid::Uuid::new_v4().to_string();
        self.disk_uuid = Some(id);
    }

    pub fn get_disk_uuid(&self) -> Option<String> {
        self.disk_uuid.clone()
    }

    pub fn set_batch_id(&mut self, batch_id: u64) {
        self.batch_id = batch_id;
    }

    pub fn get_batch_id(&self) -> u64 {
        self.batch_id
    }

    pub fn set_restored(&mut self) {
        self.was_restored = true;
    }

    pub fn was_restored(&self) -> bool {
        self.was_restored
    }

    pub fn from_bytes(bytes: &[u8]) -> Self
    where
        T: Serialize + DeserializeOwned,
    {
        deserialize(bytes).unwrap()
    }

    pub fn to_bytes(&self) -> Vec<u8>
    where
        T: Serialize + DeserializeOwned,
    {
        serialize(&self).unwrap()
    }
}
