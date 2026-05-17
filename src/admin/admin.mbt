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
} derive(Eq, ToJson, Debug)

///|
pub struct AdminClient[B] {
  backend : B
}

///|
pub fn[B] AdminClient::new(backend : B) -> AdminClient[B] {
  { backend, }
}

///|
pub fn[B : @backend.QueueBackend] AdminClient::pause_queue(
  self : AdminClient[B],
  queue_name : String,
  reason : String,
) -> Unit raise {
  self.backend.pause(queue_name, reason)
}

///|
pub fn[B : @backend.QueueBackend] AdminClient::resume_queue(
  self : AdminClient[B],
  queue_name : String,
) -> Unit raise {
  self.backend.resume_queue(queue_name)
}

///|
pub fn[B : @backend.QueueBackend] AdminClient::queue_snapshot(
  self : AdminClient[B],
  queue_name : String,
) -> QueueAdminSnapshot raise {
  { queue_name, stats: self.backend.stats(queue_name) }
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
pub fn[B : @backend.QueueBackend] AdminClient::metrics(
  self : AdminClient[B],
  consumer : @consumer.ConsumerService[B],
) -> ConsumerMetricsSnapshot raise {
  let consumer_snapshot = consumer.snapshot()
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
  }
}
