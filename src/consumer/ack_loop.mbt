///|
pub fn[B : @backend.QueueBackend] ConsumerService::ack_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
) -> Unit raise {
  self.backend.ack(lease.lease_id, (self.clock.now_ms)())
  mark_lease_finished(self, lease.lease_id)
}

///|
pub fn[B : @backend.QueueBackend] ConsumerService::nack_retry_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
  error_message : String,
  next_visible_at_ms : Int64,
) -> Unit raise {
  self.backend.nack_retry(
    lease.lease_id,
    error_message,
    next_visible_at_ms,
    (self.clock.now_ms)(),
  )
  mark_lease_finished(self, lease.lease_id)
}

///|
pub fn[B : @backend.QueueBackend] ConsumerService::nack_dead_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
  error_message : String,
) -> Unit raise {
  self.backend.nack_dead(lease.lease_id, error_message, (self.clock.now_ms)())
  mark_lease_finished(self, lease.lease_id)
}
