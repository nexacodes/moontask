///|
/// Lifecycle hook for observing task execution around handler invocation.
pub(open) trait Hook {
  /// Called before a leased job is dispatched to the registered handler.
  before_process(Self, ProcessEvent) -> Unit raise
  /// Called after a handler attempt completes, succeeds, fails, or is cancelled.
  after_process(Self, ProcessEvent, ProcessResult) -> Unit raise
}

///|
/// Immutable snapshot describing the attempt that is about to be processed.
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
/// Final outcome for a single processing attempt.
pub(all) enum AttemptOutcome {
  Succeeded
  FailedWillRetry
  FailedDead
  Cancelled
} derive(Eq, ToJson, Debug)

///|
/// Summary returned to hooks after an attempt finishes.
pub(all) struct ProcessResult {
  outcome : AttemptOutcome
  error_message : String?
  duration_ms : Int64
  finished_at_ms : Int64
  next_visible_at_ms : Int64?
} derive(Eq, ToJson, Debug)
