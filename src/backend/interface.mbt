///|
pub(open) trait QueueBackend {
  async enqueue(Self, @core.JobRecord) -> @core.JobId
  async reserve_batch(
    Self,
    queue_name : String,
    consumer_id : String,
    limit : Int,
    lease_ms : Int,
    now_ms : Int64,
  ) -> Array[@core.Lease]
  async mark_running(
    Self,
    lease_id : String,
    started_at_ms : Int64,
    deadline_ms : Int64?,
  ) -> @core.Lease
  async ack(Self, lease_id : String, finished_at_ms : Int64) -> Unit
  async nack_retry(
    Self,
    lease_id : String,
    error_message : String,
    next_visible_at_ms : Int64,
    finished_at_ms : Int64,
  ) -> Unit
  async nack_dead(
    Self,
    lease_id : String,
    error_message : String,
    finished_at_ms : Int64,
  ) -> Unit
  async extend_lease(Self, lease_id : String, new_deadline_ms : Int64) -> Unit
  async reap_expired_leases(Self, now_ms : Int64) -> Int
  async pause(Self, queue_name : String, reason : String) -> Unit
  async resume_queue(Self, queue_name : String) -> Unit
  async stats(Self, queue_name : String) -> @core.StatsSnapshot
  async list_jobs(
    Self,
    queue_name : String,
    status : @core.JobStatus?,
    limit : Int,
    cursor : String?,
  ) -> @core.JobPage
  async get_job(Self, job_id : String) -> @core.JobRecord?
  async retry_dead_job(Self, job_id : String, now_ms : Int64) -> Bool
  async cancel_job(Self, job_id : String, finished_at_ms : Int64) -> Bool
  async drain_queue(Self, queue_name : String, finished_at_ms : Int64) -> Int
  async health(Self) -> @core.BackendHealth
}
