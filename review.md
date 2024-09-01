# Code review

```diff
  /// RPQ hold private items and configuration for the RPQ.
- /// You don't need to interact with the items in this struct directly,
- /// but instead via the implementations attched to the RPQ struct.
```

No need to say that since the struct elements are not `pub`

---

```rust
    // buckets is a map of priorities to a binary heap of items
    buckets: Arc<HashMap<usize, pq::PriorityQueue<T>>>,
```

- Why is it a map? We have priorities [0..max_priority), so we could use an array here.
- This doesn't need to be Arc I don't think
- Changing this to `Vec<pq::PriorityQueue<T>>` has dropped down runtime for 100m messages from
  5.5s/5.5s to 4.2s/4.2s

---

```rust
    pub async fn new(options: RPQOptions) -> Result<(Arc<RPQ<T>>, usize), Box<dyn Error>>{
```

This function is way too big, split it up. For example, the disk cache loading should be its own
function.

## `mut` variables

In Rust, it's best practive to avoid as much mutable state as convenient. Having non-mutable vars
makes it easy to reason about the code.

Some examples:

```rust
pub async fn len(&self) -> usize {
    let mut len = 0 as usize;
    for (_, active_bucket) in self.buckets.iter() {
        len += active_bucket.len();
    }
    len
}
```

can be written as

```rust
pub async fn len(&self) -> usize {
    self.buckets.iter().map(|active_bucket| active_bucket.len()).sum()
}
```

Similarly, vars can often immediately initialized, taking advantage of the fact that most things
in Rust are expressions:

```rust
let disk_cache: Option<Arc<Database>>;
if disk_cache_enabled {
    let db = Database::create(&path).unwrap();
    let db = Arc::new(db);
    disk_cache = Some(db);
} else {
    disk_cache = None;
}
```

can be written as

```rust
let disk_cache = if disk_cache_enabled {
        let db = Database::create(&path).unwrap();
        let db = Arc::new(db);
        Some(db)
    } else {
        None
    };
```

or even

```rust
let disk_cache = disk_cache_enabled.then(|| {
    let db = Database::create(&path).unwrap();
    Arc::new(db);
});
```

## Error handling

You use `Result<..., Box<dyn Error>>` as return type for public functions, but you don't take
advantage of the `?` operator at all, instead preferring `unwrap()`. `unwrap()` and `expect()`
should be reserved for things that definitely shouldn't fail, and if they do it's catastrophic.

An example on a thing it's worth to unwrap on would be when we read a priority from
`non_empty_buckets` - it would be a logic error if there was no bucket for that priority.

The API will be more ergonomic by concretely specifying the error return type of the functions
instead of `Box<dyn Error>`. `IoError` might be fine, but consider a custom error type.

---

```rust
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
```

Not sure what to do with that yet but erroring on stdout is probably not the
right way to handle the errors here. `panic!` might be more apt.

---

```rust
        // Fetch the item from the bucket
        let item = queue.dequeue();
        if item.is_none() {
            return Result::Err(Box::<dyn Error>::from(IoError::new(
                ErrorKind::InvalidInput,
                "No items in queue",
            )));
        }
```

The `dequeue()` function returns an Option. When there are no items in the queue, the right return
value would be `Ok(None)` instead of an error.

## Thread safety

```rust
// If the bucket is empty, remove it from the non_empty_buckets
if queue.len() == 0 {
    self.non_empty_buckets.remove_bucket(&bucket_id);
}
```

The need for synchronization between non_empty_buckets & buckets makes the library non-thread-safe
logic-wise. Imagine threads A & B:

```
A                                        B
|                                        |
|_ enqueue()                             |
|_ dequeue():                            |
   |_ queue.dequeue()                    |
   |_ queue.len() == 0                   |
   |                                     |_ enqueue()
   |_ non_empty_buckets.remove_bucket()
```

The whole enqueue/dequeue operation would have to be in a mutex for the current architecture to work.

Honestly, even with ~100 priorities it might be faster to do a linear queue lookup anyway instead of
relying on separate non_empty_queues. But, it only makes sense with an external mutex (so a batch)
instead of doing a separate mutex on each priorityqueue.

Reading the code, it makes much more sense to mutex the whole RPQ structure instead of having
mutexes scattered around. Multiple mutexes will be slower, and as noted above there's data coherency
issues when RPQs functions are executed in parallel.

Note the `non_empty_buckets`/`buckets` interaction is only one example, there's similar issues with
other struct members.

## Ergonomy

```rust
let rpq = RPQ::new(options).await.expect("Error Creating RPQ").0;
```

It's weird that new is async, but I guess we're starting handlers there...
