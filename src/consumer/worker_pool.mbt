///|
pub fn[B] ConsumerService::available_worker_slots(
  self : ConsumerService[B],
) -> Int {
  Int::max(0, self.options.max_concurrency - self.inflight_count)
}

///|
fn[B] mark_leases_reserved(
  consumer : ConsumerService[B],
  leases : Array[@core.Lease],
) -> Unit {
  consumer.inflight_count = consumer.inflight_count + leases.length()
  for lease in leases {
    consumer.inflight_leases[lease.lease_id] = lease
  }
}

///|
fn[B] mark_lease_running(
  consumer : ConsumerService[B],
  lease : @core.Lease,
) -> Unit {
  consumer.inflight_leases[lease.lease_id] = lease
}

///|
fn[B] mark_lease_extended(
  consumer : ConsumerService[B],
  lease_id : String,
  new_deadline_ms : Int64,
) -> Unit {
  match consumer.inflight_leases.get(lease_id) {
    Some(lease) => {
      let updated_job : @core.JobRecord = {
        ..lease.job,
        lease_deadline_ms: Some(new_deadline_ms),
      }
      consumer.inflight_leases[lease_id] = {
        ..lease,
        lease_deadline_ms: new_deadline_ms,
        job: updated_job,
      }
    }
    None => ()
  }
}

///|
fn[B] mark_lease_finished(
  consumer : ConsumerService[B],
  lease_id : String,
) -> Unit {
  if consumer.inflight_leases.contains(lease_id) {
    consumer.inflight_leases.remove(lease_id)
    consumer.inflight_count = Int::max(0, consumer.inflight_count - 1)
  }
}

///|
fn[B] inflight_leases_snapshot(
  consumer : ConsumerService[B],
) -> Array[@core.Lease] {
  let leases = []
  for lease in consumer.inflight_leases.values() {
    leases.push(lease)
  }
  leases.sort_by(fn(lhs, rhs) { lhs.lease_id.compare(rhs.lease_id) })
  leases
}

///|
fn[B] reset_inflight_tracking(consumer : ConsumerService[B]) -> Unit {
  let lease_ids = []
  for lease_id in consumer.inflight_leases.keys() {
    lease_ids.push(lease_id)
  }
  for lease_id in lease_ids {
    consumer.inflight_leases.remove(lease_id)
  }
  consumer.inflight_count = 0
}
