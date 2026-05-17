///|

///|
pub(all) struct AdminRuntimeOptions {
  clock : @core.RuntimeClock
}

///|
pub(all) struct QueueAdminSnapshot {
  queue_name : String
  stats : @core.StatsSnapshot
} derive(Eq, ToJson, Debug)

///|
pub(all) struct ConsumerMetricsSnapshot {
  queue_name : String
  consumer_id : String
  lifecycle : @consumer.ConsumerLifecycle
  inflight_count : Int
  available_worker_slots : Int
  pending : Int
  scheduled : Int
  reserved : Int
  running : Int
  retry_waiting : Int
  succeeded : Int
  retried : Int
  failed : Int
  dead : Int
  cancelled : Int
  paused : Bool
  reserve_calls : Int
  reserved_jobs : Int
  reserve_latency_ms_total : Int64
  ack_calls : Int
  ack_latency_ms_total : Int64
  nack_retry_calls : Int
  nack_dead_calls : Int
  lease_extensions : Int
  expired_leases_reaped : Int
  processed_attempts : Int
  succeeded_attempts : Int
  failed_retry_attempts : Int
  failed_dead_attempts : Int
  cancelled_attempts : Int
} derive(Eq, ToJson, Debug)

///|
pub struct AdminClient[B] {
  backend : B
  clock : @core.RuntimeClock
}

///|
pub fn default_admin_runtime_options() -> AdminRuntimeOptions {
  { clock: @core.system_runtime_clock() }
}

///|
pub fn[B] AdminClient::new(backend : B) -> AdminClient[B] {
  AdminClient::new_with_runtime(backend, default_admin_runtime_options())
}

///|
pub fn[B] AdminClient::new_with_runtime(
  backend : B,
  runtime_options : AdminRuntimeOptions,
) -> AdminClient[B] {
  { backend, clock: runtime_options.clock }
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::pause_queue(
  self : AdminClient[B],
  queue_name : String,
  reason : String,
) -> Unit {
  self.backend.pause(queue_name, reason)
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::resume_queue(
  self : AdminClient[B],
  queue_name : String,
) -> Unit {
  self.backend.resume_queue(queue_name)
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::queue_snapshot(
  self : AdminClient[B],
  queue_name : String,
) -> QueueAdminSnapshot {
  { queue_name, stats: self.backend.stats(queue_name) }
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::list_jobs(
  self : AdminClient[B],
  queue_name : String,
  status? : @core.JobStatus,
  limit : Int,
  cursor? : String,
) -> @core.JobPage {
  self.backend.list_jobs(queue_name, status, limit, cursor)
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::get_job(
  self : AdminClient[B],
  job_id : String,
) -> @core.JobRecord? {
  self.backend.get_job(job_id)
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::retry_dead_job(
  self : AdminClient[B],
  job_id : String,
) -> Bool {
  self.backend.retry_dead_job(job_id, (self.clock.now_ms)())
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::cancel_job(
  self : AdminClient[B],
  job_id : String,
) -> Bool {
  self.backend.cancel_job(job_id, (self.clock.now_ms)())
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::drain_queue(
  self : AdminClient[B],
  queue_name : String,
) -> Int {
  self.backend.drain_queue(queue_name, (self.clock.now_ms)())
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::health(
  self : AdminClient[B],
) -> @core.BackendHealth {
  self.backend.health()
}

///|
pub fn[B] AdminClient::inspect_consumer(
  self : AdminClient[B],
  consumer : @consumer.ConsumerService[B],
) -> @consumer.ConsumerInspection {
  ignore(self)
  consumer.inspect()
}

///|
pub async fn[B : @backend.QueueBackend] AdminClient::metrics(
  self : AdminClient[B],
  consumer : @consumer.ConsumerService[B],
) -> ConsumerMetricsSnapshot {
  let consumer_snapshot = consumer.snapshot()
  let runtime_metrics = consumer.runtime_metrics()
  let queue_stats = self.backend.stats(consumer_snapshot.queue_name)
  {
    queue_name: consumer_snapshot.queue_name,
    consumer_id: consumer_snapshot.consumer_id,
    lifecycle: consumer_snapshot.lifecycle,
    inflight_count: consumer_snapshot.inflight_count,
    available_worker_slots: consumer_snapshot.available_worker_slots,
    pending: queue_stats.pending,
    scheduled: queue_stats.scheduled,
    reserved: queue_stats.reserved,
    running: queue_stats.running,
    retry_waiting: queue_stats.retry_waiting,
    succeeded: queue_stats.succeeded,
    retried: queue_stats.retried,
    failed: queue_stats.failed,
    dead: queue_stats.dead,
    cancelled: queue_stats.cancelled,
    paused: queue_stats.paused,
    reserve_calls: runtime_metrics.reserve_calls,
    reserved_jobs: runtime_metrics.reserved_jobs,
    reserve_latency_ms_total: runtime_metrics.reserve_latency_ms_total,
    ack_calls: runtime_metrics.ack_calls,
    ack_latency_ms_total: runtime_metrics.ack_latency_ms_total,
    nack_retry_calls: runtime_metrics.nack_retry_calls,
    nack_dead_calls: runtime_metrics.nack_dead_calls,
    lease_extensions: runtime_metrics.lease_extensions,
    expired_leases_reaped: runtime_metrics.expired_leases_reaped,
    processed_attempts: runtime_metrics.processed_attempts,
    succeeded_attempts: runtime_metrics.succeeded_attempts,
    failed_retry_attempts: runtime_metrics.failed_retry_attempts,
    failed_dead_attempts: runtime_metrics.failed_dead_attempts,
    cancelled_attempts: runtime_metrics.cancelled_attempts,
  }
}
