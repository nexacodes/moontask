///|
pub(all) struct AdminSnapshot {
  lifecycle : @runtime.QueueLifecycle
  paused : Bool
  next_job_id : Int
  consecutive_failures : Int
  ready_job_ids : Array[@taskq.JobId]
  delayed_job_ids : Array[@taskq.JobId]
  inflight_job_ids : Array[@taskq.JobId]
  dedupe_keys : Array[String]
  rate_tokens : Double
  rate_last_refill_ms : Int64
  hook_errors : Int
} derive(ToJson, Debug)

///|
fn sorted_job_ids(ids : Array[@taskq.JobId]) -> Array[@taskq.JobId] {
  let snapshot = ids.copy()
  snapshot.sort()
  snapshot
}

///|
fn[V] sorted_map_keys(values : Map[String, V]) -> Array[String] {
  let keys = values.keys().to_array()
  keys.sort()
  keys
}

///|
pub fn MemQueue::delayed_job_count(self : MemQueue) -> Int {
  self.delayed_queue.length()
}

///|
pub fn MemQueue::dedupe_job_count(self : MemQueue) -> Int {
  self.dedupe_index.length()
}

///|
pub fn MemQueue::hook_error_count(self : MemQueue) -> Int {
  self.hook_errors
}

///|
pub fn MemQueue::admin_snapshot(self : MemQueue) -> AdminSnapshot {
  {
    lifecycle: self.lifecycle,
    paused: self.paused,
    next_job_id: self.next_job_id,
    consecutive_failures: self.consecutive_failures,
    ready_job_ids: sorted_job_ids(self.ready_queue),
    delayed_job_ids: sorted_job_ids(self.delayed_queue),
    inflight_job_ids: sorted_map_keys(self.inflight),
    dedupe_keys: sorted_map_keys(self.dedupe_index),
    rate_tokens: self.rate_tokens,
    rate_last_refill_ms: self.rate_last_refill_ms,
    hook_errors: self.hook_errors,
  }
}
