use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

pub struct BucketPriorityQueue {
    bucket_ids: RwLock<BTreeMap<u64, bool>>,
    active_buckets: AtomicU64,
}

impl BucketPriorityQueue {
    pub fn new() -> BucketPriorityQueue {
        BucketPriorityQueue {
            bucket_ids: RwLock::new(BTreeMap::new()),
            active_buckets: AtomicU64::new(0),
        }
    }

    pub fn len(&self) -> u64 {
        self.active_buckets.load(Ordering::Relaxed)
    }

    pub fn peek(&self) -> Option<u64> {
        self.bucket_ids
            .read()
            .unwrap()
            .first_key_value()
            .map(|(k, _)| *k)
    }

    pub fn add_bucket(&self, bucket_id: u64) {
        // If the bucket already exists, return
        if self.bucket_ids.read().unwrap().contains_key(&bucket_id) {
            return;
        }

        self.bucket_ids.write().unwrap().insert(bucket_id, true);
        self.active_buckets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove_bucket(&self, bucket_id: u64) {
        self.bucket_ids.write().unwrap().remove(&bucket_id);
        self.active_buckets.fetch_sub(1, Ordering::Relaxed);
    }
}
