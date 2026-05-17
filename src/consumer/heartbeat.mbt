///|
pub fn[B : @backend.QueueBackend] ConsumerService::extend_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
  new_deadline_ms : Int64,
) -> Unit raise {
  self.backend.extend_lease(lease.lease_id, new_deadline_ms)
  self.lease_extensions = self.lease_extensions + 1
  mark_lease_extended(self, lease.lease_id, new_deadline_ms)
}

///|
pub fn[B : @backend.QueueBackend] ConsumerService::reap_expired_leases(
  self : ConsumerService[B],
) -> Int raise {
  self.backend.reap_expired_leases((self.clock.now_ms)())
}

///|
pub fn[B : @backend.QueueBackend] ConsumerService::mark_running_lease(
  self : ConsumerService[B],
  lease : @core.Lease,
  started_at_ms : Int64,
  deadline_ms : Int64?,
) -> @core.Lease raise {
  let running_lease = self.backend.mark_running(
    lease.lease_id,
    started_at_ms,
    deadline_ms,
  )
  mark_lease_running(self, running_lease)
  running_lease
}

///|
fn[B] should_run_heartbeat(
  consumer : ConsumerService[B],
  now_ms : Int64,
) -> Bool {
  match consumer.last_heartbeat_at_ms {
    Some(last_heartbeat_at_ms) =>
      now_ms - last_heartbeat_at_ms >=
      Int64::from_int(consumer.options.heartbeat_interval_ms)
    None => true
  }
}

///|
fn[B] should_run_reaper(consumer : ConsumerService[B], now_ms : Int64) -> Bool {
  match consumer.last_reaper_at_ms {
    Some(last_reaper_at_ms) =>
      now_ms - last_reaper_at_ms >=
      Int64::from_int(consumer.options.reaper_interval_ms)
    None => true
  }
}

///|
pub fn[B : @backend.QueueBackend] ConsumerService::run_heartbeat_cycle(
  self : ConsumerService[B],
) -> Unit raise {
  let now_ms = (self.clock.now_ms)()
  guard should_run_heartbeat(self, now_ms) else { return }
  let leases = inflight_leases_snapshot(self)
  if leases.length() > 0 {
    let new_deadline_ms = now_ms + Int64::from_int(self.options.lease_ms)
    for lease in leases {
      self.extend_lease(lease, new_deadline_ms)
    }
  }
  self.last_heartbeat_at_ms = Some(now_ms)
}

///|
pub fn[B : @backend.QueueBackend] ConsumerService::run_reaper_cycle(
  self : ConsumerService[B],
) -> Unit raise {
  let now_ms = (self.clock.now_ms)()
  guard should_run_reaper(self, now_ms) else { return }
  self.expired_leases_reaped = self.expired_leases_reaped +
    self.backend.reap_expired_leases(now_ms)
  self.last_reaper_at_ms = Some(now_ms)
}
