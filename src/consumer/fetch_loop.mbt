///|
pub fn[B : @backend.QueueBackend] ConsumerService::reserve_once(
  self : ConsumerService[B],
) -> Array[@core.Lease] raise {
  guard self.lifecycle == Running else { return [] }
  let available_slots = self.available_worker_slots()
  guard available_slots > 0 else { return [] }
  let batch_size = Int::min(self.options.reserve_batch_size, available_slots)
  let leases = self.backend.reserve_batch(
    self.options.queue_name,
    self.options.consumer_id,
    batch_size,
    self.options.lease_ms,
    (self.clock.now_ms)(),
  )
  mark_leases_reserved(self, leases)
  leases
}

///|
pub async fn[B] ConsumerService::sleep_until_next_poll(
  self : ConsumerService[B],
) -> Unit {
  let delay_ms = if self.lifecycle == Running {
    self.options.poll_interval_ms
  } else {
    self.options.pause_poll_interval_ms
  }
  (self.clock.sleep_ms)(delay_ms)
}
