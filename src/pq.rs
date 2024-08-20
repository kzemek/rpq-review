use std::time::{Duration, Instant};
use std::{collections::VecDeque, sync::RwLock};

pub struct PriorityQueue<T: Ord + Clone> {
    items: RwLock<VecDeque<Item<T>>>,
}

impl<T: Ord + Clone> PriorityQueue<T> {
    pub fn new() -> PriorityQueue<T> {
        PriorityQueue {
            items: RwLock::new(VecDeque::new()),
        }
    }

    pub fn len(&self) -> u64 {
        self.items.read().unwrap().len() as u64
    }

    pub fn enqueue(&self, item: Item<T>) {
        let mut item = item;

        // Set the internal fields
        item.submitted_at = Instant::now();
        item.last_escalation = None;
        item.was_restored = false;

        // Add the item to the queue
        self.items.write().unwrap().push_back(item);
    }

    pub fn dequeue(&self) -> Option<Item<T>> {
        self.items.write().unwrap().pop_front()
    }

    pub fn peek(&self) -> Option<Item<T>> {
        self.items.read().unwrap().front().cloned()
    }

    pub fn prioritize(&self) {
        let mut items = self.items.write().unwrap();
        let mut to_remove = Vec::new();
        let mut to_swap = Vec::new();

        for (index, item) in items.iter_mut().enumerate() {
            // Timeout items that have been in the queue for too long
            if item.can_timeout {
                if item.timeout.unwrap().as_secs() > item.submitted_at.elapsed().as_secs() {
                    to_remove.push(index);
                    continue;
                }
            }

            // Escalate items that have been in the queue for too long
            if item.should_escalate {
                // Check if we have ever escalated this item
                if item.last_escalation.is_none() {
                    if item.escalation_rate.unwrap().as_secs()
                        > item.submitted_at.elapsed().as_secs()
                    {
                        item.last_escalation = Some(Instant::now());
                        if index > 0 {
                            to_swap.push(index);
                        }
                        continue;
                    }
                } else {
                    // Check if we need to escalate this item again
                    if item.escalation_rate.unwrap().as_secs()
                        > item.last_escalation.unwrap().elapsed().as_secs()
                    {
                        item.last_escalation = Some(Instant::now());
                        if index > 0 {
                            to_swap.push(index);
                        }
                        continue;
                    }
                }
            }
        }

        // Perform removals and swaps
        for index in to_remove.iter().rev() {
            items.remove(*index);
        }
        for index in to_swap {
            items.swap(index, index - 1);
        }
    }
}

// Item is a struct that holds the data and metadata for an item in the queue
#[derive(Clone)]
pub struct Item<T: Clone> {
    // User
    pub priority: u64,
    pub data: T,
    pub disk_uuid: Option<String>,
    pub should_escalate: bool,
    pub escalation_rate: Option<Duration>,
    pub can_timeout: bool,
    pub timeout: Option<Duration>,

    // Internal
    submitted_at: Instant,
    last_escalation: Option<Instant>,
    batch_id: u64,
    was_restored: bool,
}

impl<T: Clone> Item<T> {
    // Constructor to initialize the struct
    pub fn new(
        priority: u64,
        data: T,
        disk_uuid: Option<String>,
        should_escalate: bool,
        escalation_rate: Option<Duration>,
        can_timeout: bool,
        timeout: Option<Duration>,
    ) -> Self {
        Item {
            // User-provided fields
            priority,
            data,
            disk_uuid,
            should_escalate,
            escalation_rate,
            can_timeout,
            timeout,

            // Internal fields
            submitted_at: Instant::now(),
            last_escalation: None,
            batch_id: 0,
            was_restored: false,
        }
    }
}
