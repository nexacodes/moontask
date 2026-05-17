///|
/// Clock abstraction used by runtimes so production code can use wall-clock
/// time while tests inject deterministic time.
pub(all) struct RuntimeClock {
  now_ms : () -> Int64
  sleep_ms : async (Int) -> Unit
}

///|
/// Returns the default runtime clock backed by `moonbitlang/async`.
pub fn system_runtime_clock() -> RuntimeClock {
  {
    now_ms: fn() { @async.now() },
    sleep_ms: async fn(delay_ms : Int) { @async.sleep(delay_ms) },
  }
}
