///|
pub struct FakeClock {
  mut current_ms : Int64
  sleep_history_ms : Array[Int]
}

///|
pub fn new_fake_clock(start_ms : Int64) -> FakeClock {
  { current_ms: start_ms, sleep_history_ms: [] }
}

///|
pub fn FakeClock::runtime_clock(self : FakeClock) -> @taskq.RuntimeClock {
  {
    now_ms: fn() { self.current_ms },
    sleep_ms: fn(delay_ms : Int) {
      self.sleep_history_ms.push(delay_ms)
      self.current_ms = self.current_ms + Int64::from_int(delay_ms)
    },
  }
}

///|
pub fn FakeClock::now_ms(self : FakeClock) -> Int64 {
  self.current_ms
}

///|
pub fn FakeClock::advance(self : FakeClock, delta_ms : Int) -> Unit {
  self.current_ms = self.current_ms + Int64::from_int(delta_ms)
}

///|
pub fn FakeClock::sleep_history(self : FakeClock) -> Array[Int] {
  self.sleep_history_ms.copy()
}

///|
pub fn FakeClock::total_slept_ms(self : FakeClock) -> Int {
  let mut total = 0
  for delay_ms in self.sleep_history_ms {
    total = total + delay_ms
  }
  total
}
