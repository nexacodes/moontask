///|
/// Durable queue contract implemented by storage backends such as memory or Redis.
pub(open) trait QueueBackend {
  /// Persists a new job and returns its stable identifier.
  async enqueue(Self, @core.JobRecord) -> @core.JobId
  /// Reserves up to `limit` visible jobs for `consumer_id` and attaches a lease.
  async reserve_batch(
    Self,
    queue_name : String,
    consumer_id : String,
    limit : Int,
    lease_ms : Int,
    now_ms : Int64,
  ) -> Array[@core.Lease]
  /// Marks a reserved lease as actively running and optionally records a deadline.
  async mark_running(
    Self,
    lease_id : String,
    started_at_ms : Int64,
    deadline_ms : Int64?,
  ) -> @core.Lease
  /// Acknowledges a successfully processed lease and marks the job complete.
  async ack(Self, lease_id : String, finished_at_ms : Int64) -> Unit
  /// Moves a failed lease back to a visible retry state.
  async nack_retry(
    Self,
    lease_id : String,
    error_message : String,
    next_visible_at_ms : Int64,
    finished_at_ms : Int64,
  ) -> Unit
  /// Marks a failed lease as dead and terminal.
  async nack_dead(
    Self,
    lease_id : String,
    error_message : String,
    finished_at_ms : Int64,
  ) -> Unit
  /// Extends the deadline of an active lease.
  async extend_lease(Self, lease_id : String, new_deadline_ms : Int64) -> Unit
  /// Reclaims expired leases and returns how many were reaped.
  async reap_expired_leases(Self, now_ms : Int64) -> Int
  /// Prevents new reservations from a queue until resumed.
  async pause(Self, queue_name : String, reason : String) -> Unit
  /// Re-enables reservations for a paused queue.
  async resume_queue(Self, queue_name : String) -> Unit
  /// Returns aggregate statistics for a queue.
  async stats(Self, queue_name : String) -> @core.StatsSnapshot
  /// Lists jobs in queue order with optional status filtering and cursor pagination.
  async list_jobs(
    Self,
    queue_name : String,
    status : @core.JobStatus?,
    limit : Int,
    cursor : String?,
  ) -> @core.JobPage
  /// Loads a job by id when it exists.
  async get_job(Self, job_id : String) -> @core.JobRecord?
  /// Requeues a dead job and returns whether a state change happened.
  async retry_dead_job(Self, job_id : String, now_ms : Int64) -> Bool
  /// Cancels a single job and returns whether it changed state.
  async cancel_job(Self, job_id : String, finished_at_ms : Int64) -> Bool
  /// Cancels all non-terminal jobs in a queue and returns the count affected.
  async drain_queue(Self, queue_name : String, finished_at_ms : Int64) -> Int
  /// Reports connectivity and implementation-specific health information.
  async health(Self) -> @core.BackendHealth
}
