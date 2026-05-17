///|
pub(open) trait QueueBackend {
  enqueue(Self, @core.JobRecord) -> @core.JobId raise
  reserve_batch(
    Self,
    queue_name : String,
    consumer_id : String,
    limit : Int,
    lease_ms : Int,
    now_ms : Int64,
  ) -> Array[@core.Lease] raise
  mark_running(
    Self,
    lease_id : String,
    started_at_ms : Int64,
    deadline_ms : Int64?,
  ) -> @core.Lease raise
  ack(Self, lease_id : String, finished_at_ms : Int64) -> Unit raise
  nack_retry(
    Self,
    lease_id : String,
    error_message : String,
    next_visible_at_ms : Int64,
    finished_at_ms : Int64,
  ) -> Unit raise
  nack_dead(
    Self,
    lease_id : String,
    error_message : String,
    finished_at_ms : Int64,
  ) -> Unit raise
  extend_lease(Self, lease_id : String, new_deadline_ms : Int64) -> Unit raise
  reap_expired_leases(Self, now_ms : Int64) -> Int raise
  pause(Self, queue_name : String, reason : String) -> Unit raise
  resume_queue(Self, queue_name : String) -> Unit raise
  stats(Self, queue_name : String) -> @core.StatsSnapshot raise
  list_jobs(
    Self,
    queue_name : String,
    status : @core.JobStatus?,
    limit : Int,
    cursor : String?,
  ) -> @core.JobPage raise
  get_job(Self, job_id : String) -> @core.JobRecord? raise
  retry_dead_job(Self, job_id : String, now_ms : Int64) -> Bool raise
  cancel_job(Self, job_id : String, finished_at_ms : Int64) -> Bool raise
  drain_queue(Self, queue_name : String, finished_at_ms : Int64) -> Int raise
  health(Self) -> @core.BackendHealth raise
}
