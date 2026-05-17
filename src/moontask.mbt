///|
/// Root facade package that re-exports the most commonly used `moontask` APIs.
pub using @core {
  bytes_codec,
  default_enqueue_options,
  empty_stats_snapshot,
  retry_policy_max_attempts,
  system_runtime_clock,
  trait Hook,
  type AttemptOutcome,
  type BackendHealth,
  type Codec,
  type EnqueueOptions,
  type JobId,
  type JobPage,
  type JobRecord,
  type JobStatus,
  type Lease,
  type ProcessEvent,
  type ProcessResult,
  type QueueError,
  type RetryPolicy,
  type RuntimeClock,
  type StatsSnapshot,
  type Task,
  type TaskContext,
  type TaskOptions,
}

///|
pub using @backend {trait QueueBackend}

///|
pub using @registry {
  max_attempts_for_task,
  new_registry,
  register_task,
  registry_retry_policy,
  type Registry,
}

///|
pub using @producer {
  default_producer_runtime_options,
  type Producer,
  type ProducerRuntimeOptions,
}

///|
pub using @consumer {
  default_consumer_options,
  default_consumer_runtime_options,
  type ConsumerInspection,
  type ConsumerLifecycle,
  type ConsumerOptions,
  type ConsumerRuntimeMetrics,
  type ConsumerRuntimeOptions,
  type ConsumerService,
  type ConsumerSnapshot,
}

///|
pub using @admin {
  default_admin_runtime_options,
  type AdminClient,
  type AdminRuntimeOptions,
  type ConsumerMetricsSnapshot,
  type QueueAdminSnapshot,
}

///|
pub using @memory {new_mem_backend, type MemBackend}

///|
pub using @redis {
  connect,
  new_pool,
  new_redis_backend,
  type RedisArray,
  type RedisBackend,
  type RedisBulkString,
  type RedisClient,
  type RedisClientPool,
  type RedisHash,
  type RedisInt,
  type RedisString,
  type RedisValue,
}
