///|
pub fn[B] ConsumerService::is_accepting_work(self : ConsumerService[B]) -> Bool {
  self.lifecycle == Idle ||
  self.lifecycle == Starting ||
  self.lifecycle == Running
}
