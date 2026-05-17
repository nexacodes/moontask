///|
pub(open) trait Hook {
  before_process(Self, ProcessEvent) -> Unit raise
  after_process(Self, ProcessEvent, ProcessResult) -> Unit raise
}

///|
pub(all) struct ProcessEvent {
  queue_name : String
  task_name : String
  job_id : String
  dedupe_key : String?
  attempt : Int
  max_attempts : Int
  enqueued_at_ms : Int64
  visible_at_ms : Int64
  started_at_ms : Int64
  deadline_ms : Int64?
} derive(Eq, ToJson, Debug)

///|
pub(all) enum AttemptOutcome {
  Succeeded
  FailedWillRetry
  FailedDead
  Cancelled
} derive(Eq, ToJson, Debug)

///|
pub(all) struct ProcessResult {
  outcome : AttemptOutcome
  error_message : String?
  duration_ms : Int64
  finished_at_ms : Int64
  next_visible_at_ms : Int64?
} derive(Eq, ToJson, Debug)
