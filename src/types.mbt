///|
pub(all) struct Codec[A] {
  encode : (A) -> Bytes raise
  decode : (Bytes) -> A raise
}

///|
pub(all) struct Task[A] {
  id : Int
  name : String
  codec : Codec[A]
  retry : RetryPolicy
}

///|
pub(all) struct TaskContext {
  queue_name : String
  job_id : JobId
  attempt : Int
  max_attempts : Int
  enqueued_at_ms : Int64
  started_at_ms : Int64
  deadline_ms : Int64?
  dedupe_key : String?
} derive(Eq, ToJson, Debug)

///|
pub(all) enum RetryPolicy {
  NoRetry
  Fixed(max_attempts~ : Int, delay_ms~ : Int)
  Exponential(
    max_attempts~ : Int,
    initial_delay_ms~ : Int,
    factor~ : Double,
    max_delay_ms~ : Int
  )
} derive(Eq, ToJson, Debug)

///|
pub(all) struct TaskOptions[A] {
  name : String
  codec : Codec[A]
  retry : RetryPolicy
  handler : async (TaskContext, A) -> Unit
  fallback : (async (TaskContext, A, Error) -> Unit)?
}

///|
pub(all) struct EnqueueOptions {
  delay_ms : Int
  dedupe_key : String?
  schedule_at_ms : Int64?
  timeout_ms : Int?
  max_attempts_override : Int?
} derive(Eq, ToJson, Debug)

///|
pub(all) struct QueueOptions {
  queue_name : String
  max_concurrency : Int
} derive(Eq, ToJson, Debug)

///|
pub(all) struct JobEnvelope {
  id : JobId
  task_name : String
  payload : Bytes
  dedupe_key : String?
  created_at_ms : Int64
  visible_at_ms : Int64
  deadline_ms : Int64?
  attempt : Int
  max_attempts : Int
  last_error : String?
} derive(Eq, ToJson, Debug)

///|
pub(all) enum JobState {
  Accepted
  RejectedDuplicate
  RejectedQueueClosed
  Delayed
  Ready
  Running
  RetryScheduled
  Paused
  Succeeded
  Dead
  Cancelled
} derive(Eq, ToJson, Debug)

///|
pub(all) struct StatsSnapshot {
  queued : Int
  delayed : Int
  inflight : Int
  processed : Int
  succeeded : Int
  retried : Int
  failed : Int
  dead : Int
  active_workers : Int
  paused : Bool
} derive(Eq, ToJson, Debug)
