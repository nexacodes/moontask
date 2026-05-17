///|
/// In-memory backend intended for tests, examples, and single-process use.
pub struct MemBackend {
  mut next_lease_id : Int
  jobs : Map[String, @core.JobRecord]
  leases : Map[String, @core.Lease]
  paused_queues : Map[String, String]
  dedupe_index : Map[String, @core.JobId]
}

///|
/// Creates an empty in-memory backend.
pub fn new_mem_backend() -> MemBackend {
  {
    next_lease_id: 1,
    jobs: {},
    leases: {},
    paused_queues: {},
    dedupe_index: {},
  }
}

///|
fn is_active_status(status : @core.JobStatus) -> Bool {
  match status {
    @core.Pending => true
    @core.Scheduled => true
    @core.Reserved => true
    @core.Running => true
    @core.RetryWaiting => true
    _ => false
  }
}

///|
fn initial_job_status(job : @core.JobRecord) -> @core.JobStatus {
  if job.visible_at_ms > job.created_at_ms {
    @core.Scheduled
  } else {
    @core.Pending
  }
}

///|
fn clear_job_lease(job : @core.JobRecord) -> @core.JobRecord {
  {
    ..job,
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    deadline_ms: None,
  }
}

///|
fn remove_dedupe_if_terminal(
  backend : MemBackend,
  job : @core.JobRecord,
) -> Unit {
  if !is_active_status(job.status) {
    match job.dedupe_key {
      Some(dedupe_key) => backend.dedupe_index.remove(dedupe_key)
      None => ()
    }
  }
}

///|
fn upsert_dedupe_if_active(
  backend : MemBackend,
  job : @core.JobRecord,
) -> Unit raise {
  if is_active_status(job.status) {
    match job.dedupe_key {
      Some(dedupe_key) =>
        if backend.dedupe_index.contains(dedupe_key) &&
          backend.dedupe_index[dedupe_key] != job.job_id {
          raise @core.QueueError::DuplicateJob(dedupe_key)
        } else {
          backend.dedupe_index[dedupe_key] = job.job_id
        }
      None => ()
    }
  }
}

///|
fn update_job(
  backend : MemBackend,
  job_id : String,
  update : (@core.JobRecord) -> @core.JobRecord raise,
) -> Unit raise {
  let job = match backend.jobs.get(job_id) {
    Some(job) => job
    None => raise @core.QueueError::StateInvariant("missing job \{job_id}")
  }
  let updated = update(job)
  backend.jobs[job_id] = updated
  remove_dedupe_if_terminal(backend, updated)
}

///|
fn build_stats(
  backend : MemBackend,
  queue_name : String,
) -> @core.StatsSnapshot {
  let mut pending = 0
  let mut scheduled = 0
  let mut reserved = 0
  let mut running = 0
  let mut retry_waiting = 0
  let mut succeeded = 0
  let mut retried = 0
  let mut failed = 0
  let mut dead = 0
  let mut cancelled = 0
  for job in backend.jobs.values() {
    if job.queue_name == queue_name {
      match job.status {
        @core.Pending => pending = pending + 1
        @core.Scheduled => scheduled = scheduled + 1
        @core.Reserved => reserved = reserved + 1
        @core.Running => running = running + 1
        @core.RetryWaiting => {
          retry_waiting = retry_waiting + 1
          retried = retried + 1
        }
        @core.Succeeded => succeeded = succeeded + 1
        @core.Dead => {
          dead = dead + 1
          failed = failed + 1
        }
        @core.Cancelled => cancelled = cancelled + 1
      }
    }
  }
  {
    pending,
    scheduled,
    reserved,
    running,
    retry_waiting,
    succeeded,
    retried,
    failed,
    dead,
    cancelled,
    paused: backend.paused_queues.contains(queue_name),
  }
}

///|
fn collect_reservable_jobs(
  backend : MemBackend,
  queue_name : String,
  now_ms : Int64,
) -> Array[@core.JobRecord] {
  let jobs = []
  for job in backend.jobs.values() {
    if job.queue_name == queue_name &&
      (
        job.status is @core.Pending ||
        job.status is @core.Scheduled ||
        job.status is @core.RetryWaiting
      ) &&
      job.visible_at_ms <= now_ms {
      jobs.push(job)
    }
  }
  jobs.sort_by(fn(lhs, rhs) {
    let visible_cmp = lhs.visible_at_ms.compare(rhs.visible_at_ms)
    if visible_cmp == 0 {
      lhs.job_id.compare(rhs.job_id)
    } else {
      visible_cmp
    }
  })
  jobs
}

///|
fn collect_jobs_page(
  backend : MemBackend,
  queue_name : String,
  status : @core.JobStatus?,
  limit : Int,
  cursor : String?,
) -> @core.JobPage {
  let jobs : Array[@core.JobRecord] = []
  for job in backend.jobs.values() {
    let matches_status = match status {
      Some(status) => job.status == status
      None => true
    }
    let after_cursor = match cursor {
      Some(cursor) => job.job_id > cursor
      None => true
    }
    if job.queue_name == queue_name && matches_status && after_cursor {
      jobs.push(job)
    }
  }
  jobs.sort_by(fn(lhs, rhs) { lhs.job_id.compare(rhs.job_id) })
  let page_size = Int::max(0, limit)
  let page : Array[@core.JobRecord] = []
  for i in 0..<Int::min(page_size, jobs.length()) {
    page.push(jobs[i])
  }
  let next_cursor : String? = if jobs.length() > page.length() &&
    !page.is_empty() {
    Some(page[page.length() - 1].job_id)
  } else {
    None
  }
  { jobs: page, next_cursor }
}

///|
fn cancel_job_record(
  backend : MemBackend,
  job : @core.JobRecord,
  finished_at_ms : Int64,
) -> Unit raise {
  update_job(backend, job.job_id, fn(current) {
    {
      ..clear_job_lease(current),
      status: @core.Cancelled,
      finished_at_ms: Some(finished_at_ms),
    }
  })
  match job.lease_id {
    Some(lease_id) => backend.leases.remove(lease_id)
    None => ()
  }
}

///|
pub impl @backend.QueueBackend for MemBackend with enqueue(self, job) raise {
  let normalized_job : @core.JobRecord = {
    ..job,
    status: initial_job_status(job),
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    started_at_ms: None,
    finished_at_ms: None,
  }
  upsert_dedupe_if_active(self, normalized_job)
  self.jobs[normalized_job.job_id] = normalized_job
  normalized_job.job_id
}

///|
pub impl @backend.QueueBackend for MemBackend with reserve_batch(
  self,
  queue_name,
  consumer_id,
  limit,
  lease_ms,
  now_ms,
) raise {
  if self.paused_queues.contains(queue_name) {
    return []
  }
  let leases = []
  let candidates = collect_reservable_jobs(self, queue_name, now_ms)
  for candidate in candidates {
    guard leases.length() < limit else { break }
    let lease_id = "lease-\{self.next_lease_id}"
    self.next_lease_id = self.next_lease_id + 1
    let lease_deadline_ms = now_ms + Int64::from_int(lease_ms)
    let reserved_job : @core.JobRecord = {
      ..candidate,
      status: @core.Reserved,
      lease_id: Some(lease_id),
      lease_owner: Some(consumer_id),
      lease_deadline_ms: Some(lease_deadline_ms),
      started_at_ms: None,
      deadline_ms: None,
    }
    self.jobs[candidate.job_id] = reserved_job
    let lease : @core.Lease = {
      lease_id,
      queue_name,
      consumer_id,
      leased_at_ms: now_ms,
      lease_deadline_ms,
      job: reserved_job,
    }
    self.leases[lease_id] = lease
    leases.push(lease)
  }
  leases
}

///|
pub impl @backend.QueueBackend for MemBackend with mark_running(
  self,
  lease_id,
  started_at_ms,
  deadline_ms,
) raise {
  let lease = match self.leases.get(lease_id) {
    Some(lease) => lease
    None => raise @core.QueueError::LeaseNotFound(lease_id)
  }
  let running_job : @core.JobRecord = {
    ..lease.job,
    status: @core.Running,
    started_at_ms: Some(started_at_ms),
    deadline_ms,
  }
  self.jobs[running_job.job_id] = running_job
  let running_lease : @core.Lease = { ..lease, job: running_job }
  self.leases[lease_id] = running_lease
  running_lease
}

///|
pub impl @backend.QueueBackend for MemBackend with ack(
  self,
  lease_id,
  finished_at_ms,
) raise {
  let lease = match self.leases.get(lease_id) {
    Some(lease) => lease
    None => raise @core.QueueError::LeaseNotFound(lease_id)
  }
  update_job(self, lease.job.job_id, fn(job) {
    {
      ..clear_job_lease(job),
      status: @core.Succeeded,
      finished_at_ms: Some(finished_at_ms),
    }
  })
  self.leases.remove(lease_id)
}

///|
pub impl @backend.QueueBackend for MemBackend with nack_retry(
  self,
  lease_id,
  error_message,
  next_visible_at_ms,
  finished_at_ms,
) raise {
  let lease = match self.leases.get(lease_id) {
    Some(lease) => lease
    None => raise @core.QueueError::LeaseNotFound(lease_id)
  }
  update_job(self, lease.job.job_id, fn(job) {
    {
      ..clear_job_lease(job),
      status: if next_visible_at_ms <= finished_at_ms {
        @core.Pending
      } else {
        @core.RetryWaiting
      },
      attempt: job.attempt + 1,
      visible_at_ms: next_visible_at_ms,
      finished_at_ms: Some(finished_at_ms),
      last_error: Some(error_message),
    }
  })
  self.leases.remove(lease_id)
}

///|
pub impl @backend.QueueBackend for MemBackend with nack_dead(
  self,
  lease_id,
  error_message,
  finished_at_ms,
) raise {
  let lease = match self.leases.get(lease_id) {
    Some(lease) => lease
    None => raise @core.QueueError::LeaseNotFound(lease_id)
  }
  update_job(self, lease.job.job_id, fn(job) {
    {
      ..clear_job_lease(job),
      status: @core.Dead,
      finished_at_ms: Some(finished_at_ms),
      last_error: Some(error_message),
    }
  })
  self.leases.remove(lease_id)
}

///|
pub impl @backend.QueueBackend for MemBackend with extend_lease(
  self,
  lease_id,
  new_deadline_ms,
) raise {
  let lease = match self.leases.get(lease_id) {
    Some(lease) => lease
    None => return
  }
  let updated_lease : @core.Lease = {
    ..lease,
    lease_deadline_ms: new_deadline_ms,
  }
  self.leases[lease_id] = updated_lease
  update_job(self, lease.job.job_id, fn(job) {
    { ..job, lease_deadline_ms: Some(new_deadline_ms) }
  })
}

///|
pub impl @backend.QueueBackend for MemBackend with reap_expired_leases(
  self,
  now_ms,
) raise {
  let expired_ids = []
  for lease in self.leases.values() {
    if lease.lease_deadline_ms <= now_ms {
      expired_ids.push(lease.lease_id)
    }
  }
  for lease_id in expired_ids {
    let lease = match self.leases.get(lease_id) {
      Some(lease) => lease
      None => continue
    }
    update_job(self, lease.job.job_id, fn(job) {
      { ..clear_job_lease(job), status: @core.Pending, visible_at_ms: now_ms }
    })
    self.leases.remove(lease_id)
  }
  expired_ids.length()
}

///|
pub impl @backend.QueueBackend for MemBackend with pause(
  self,
  queue_name,
  reason,
) {
  self.paused_queues[queue_name] = reason
}

///|
pub impl @backend.QueueBackend for MemBackend with resume_queue(
  self,
  queue_name,
) {
  self.paused_queues.remove(queue_name)
}

///|
pub impl @backend.QueueBackend for MemBackend with stats(self, queue_name) {
  build_stats(self, queue_name)
}

///|
pub impl @backend.QueueBackend for MemBackend with list_jobs(
  self,
  queue_name,
  status,
  limit,
  cursor,
) {
  collect_jobs_page(self, queue_name, status, limit, cursor)
}

///|
pub impl @backend.QueueBackend for MemBackend with get_job(self, job_id) {
  self.jobs.get(job_id)
}

///|
pub impl @backend.QueueBackend for MemBackend with retry_dead_job(
  self,
  job_id,
  now_ms,
) raise {
  let job = match self.jobs.get(job_id) {
    Some(job) => job
    None => return false
  }
  guard job.status is @core.Dead else { return false }
  update_job(self, job_id, fn(current) {
    {
      ..clear_job_lease(current),
      status: @core.Pending,
      visible_at_ms: now_ms,
      finished_at_ms: None,
      last_error: current.last_error,
    }
  })
  true
}

///|
pub impl @backend.QueueBackend for MemBackend with cancel_job(
  self,
  job_id,
  finished_at_ms,
) raise {
  let job = match self.jobs.get(job_id) {
    Some(job) => job
    None => return false
  }
  match job.status {
    @core.Succeeded | @core.Dead | @core.Cancelled => false
    _ => {
      cancel_job_record(self, job, finished_at_ms)
      true
    }
  }
}

///|
pub impl @backend.QueueBackend for MemBackend with drain_queue(
  self,
  queue_name,
  finished_at_ms,
) raise {
  let job_ids = []
  for job in self.jobs.values() {
    if job.queue_name == queue_name {
      match job.status {
        @core.Succeeded | @core.Dead | @core.Cancelled => ()
        _ => job_ids.push(job.job_id)
      }
    }
  }
  for job_id in job_ids {
    let job = match self.jobs.get(job_id) {
      Some(job) => job
      None => continue
    }
    cancel_job_record(self, job, finished_at_ms)
  }
  job_ids.length()
}

///|
pub impl @backend.QueueBackend for MemBackend with health(self) {
  {
    ok: true,
    backend_kind: "memory",
    detail: Some(
      "jobs=\{self.jobs.length()} leases=\{self.leases.length()} paused=\{self.paused_queues.length()}",
    ),
  }
}
