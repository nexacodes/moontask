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
pub fn empty_stats_snapshot(paused : Bool) -> StatsSnapshot {
  {
    pending: 0,
    scheduled: 0,
    reserved: 0,
    running: 0,
    retry_waiting: 0,
    succeeded: 0,
    retried: 0,
    failed: 0,
    dead: 0,
    cancelled: 0,
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
