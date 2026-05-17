///|
pub(all) struct RuntimeClock {
  now_ms : () -> Int64
  sleep_ms : async (Int) -> Unit
}

///|
pub(all) struct QueueRuntimeOptions {
  hooks : Array[&Hook]
  clock : RuntimeClock
}

///|
pub fn system_runtime_clock() -> RuntimeClock {
  {
    now_ms: fn() { @async.now() },
    sleep_ms: async fn(delay_ms : Int) { @async.sleep(delay_ms) },
  }
}
