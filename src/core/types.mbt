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
pub(all) enum JobStatus {
  Pending
  Scheduled
  Reserved
  Running
  RetryWaiting
  Succeeded
  Dead
  Cancelled
} derive(Eq, ToJson, Debug)

///|
pub(all) struct JobRecord {
  job_id : JobId
  queue_name : String
  task_name : String
  payload : Bytes
  status : JobStatus
  attempt : Int
  max_attempts : Int
  dedupe_key : String?
  created_at_ms : Int64
  visible_at_ms : Int64
  lease_id : String?
  lease_owner : String?
  lease_deadline_ms : Int64?
  started_at_ms : Int64?
  finished_at_ms : Int64?
  timeout_ms : Int?
  deadline_ms : Int64?
  last_error : String?
  trace_id : String?
} derive(Eq, ToJson, Debug)

///|
pub(all) struct Lease {
  lease_id : String
  queue_name : String
  consumer_id : String
  leased_at_ms : Int64
  lease_deadline_ms : Int64
  job : JobRecord
} derive(Eq, ToJson, Debug)

///|
pub(all) struct StatsSnapshot {
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
