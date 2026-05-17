///|
pub async fn[B : @backend.QueueBackend] ConsumerService::reserve_once(
  self : ConsumerService[B],
) -> Array[@core.Lease] {
  guard self.lifecycle == Running else { return [] }
  let available_slots = self.available_worker_slots()
  guard available_slots > 0 else { return [] }
  let batch_size = Int::min(self.options.reserve_batch_size, available_slots)
  let started_at_ms = (self.clock.now_ms)()
  let leases = self.backend.reserve_batch(
    self.options.queue_name,
    self.options.consumer_id,
    batch_size,
    self.options.lease_ms,
    (self.clock.now_ms)(),
  )
  self.reserve_calls = self.reserve_calls + 1
  self.reserved_jobs = self.reserved_jobs + leases.length()
  self.reserve_latency_ms_total = self.reserve_latency_ms_total +
    ((self.clock.now_ms)() - started_at_ms)
  mark_leases_reserved(self, leases)
  leases
}

///|
pub async fn[B] ConsumerService::sleep_until_next_poll(
  self : ConsumerService[B],
) -> Unit {
  let base_delay_ms = if self.lifecycle == Running {
    self.options.poll_interval_ms
  } else {
    self.options.pause_poll_interval_ms
  }
  let delay_ms = match self.stop_deadline_ms {
    Some(stop_deadline_ms) if self.lifecycle == Stopping => {
      let remaining_ms = Int64::to_int(
        Int64::max(0L, stop_deadline_ms - (self.clock.now_ms)()),
      )
      Int::min(base_delay_ms, remaining_ms)
    }
    _ => base_delay_ms
  }
  guard delay_ms > 0 else { return }
  @async.with_task_group(async fn(group) {
    self.sleep_interrupt = Some(fn() {
      ignore(try? group.return_immediately(()))
    })
    defer {
      self.sleep_interrupt = None
    }
    (self.clock.sleep_ms)(delay_ms)
  })
}
