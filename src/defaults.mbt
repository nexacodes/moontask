///|
pub fn bytes_codec() -> Codec[Bytes] {
  Codec::{
    encode: fn(payload : Bytes) { payload },
    decode: fn(payload : Bytes) { payload },
  }
}

///|
pub fn default_enqueue_options() -> EnqueueOptions {
  {
    delay_ms: 0,
    dedupe_key: None,
    schedule_at_ms: None,
    timeout_ms: None,
    max_attempts_override: None,
  }
}

///|
pub fn default_queue_options(queue_name : String) -> QueueOptions {
  {
    queue_name,
    max_concurrency: 1,
    pause_after_failures: 0,
    rate_limit_per_second: None,
  }
}

///|
pub fn default_runtime_options() -> QueueRuntimeOptions {
  { hooks: [], clock: system_runtime_clock() }
}

///|
pub fn empty_stats_snapshot(paused : Bool) -> StatsSnapshot {
  {
    queued: 0,
    delayed: 0,
    inflight: 0,
    processed: 0,
    succeeded: 0,
    retried: 0,
    failed: 0,
    dead: 0,
    active_workers: 0,
    paused,
  }
}

///|
pub fn retry_policy_max_attempts(policy : RetryPolicy) -> Int {
  match policy {
    NoRetry => 1
    Fixed(max_attempts=limit, ..) => limit
    Exponential(max_attempts=limit, ..) => limit
  }
}
