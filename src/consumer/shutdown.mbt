///|
/// Returns whether the consumer may still accept newly reserved work.
pub fn[B] ConsumerService::is_accepting_work(self : ConsumerService[B]) -> Bool {
  self.lifecycle == Idle ||
  self.lifecycle == Starting ||
  self.lifecycle == Running
}

///|
fn[B] stop_deadline_from_timeout(
  consumer : ConsumerService[B],
  timeout_ms : Int,
) -> Int64 {
  (consumer.clock.now_ms)() + Int64::from_int(Int::max(0, timeout_ms))
}

///|
fn[B] ConsumerService::should_force_shutdown(self : ConsumerService[B]) -> Bool {
  match self.stop_deadline_ms {
    Some(stop_deadline_ms) =>
      self.lifecycle == Stopping &&
      self.inflight_count > 0 &&
      (self.clock.now_ms)() >= stop_deadline_ms
    None => false
  }
}

///|
fn[B] ConsumerService::should_continue_runtime(
  self : ConsumerService[B],
) -> Bool {
  match self.lifecycle {
    Idle | Stopped => false
    Starting | Running => true
    Stopping => self.inflight_count > 0 && !self.should_force_shutdown()
  }
}

///|
fn[B] ConsumerService::wake_sleeping_runtime(self : ConsumerService[B]) -> Unit {
  match self.sleep_interrupt {
    Some(interrupt) => interrupt()
    None => ()
  }
}
