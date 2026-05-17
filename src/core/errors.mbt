///|
/// Domain errors raised by queue operations and runtime invariants.
pub(all) suberror QueueError {
  DuplicateTask(String)
  DuplicateJob(String)
  TaskNotFound(String)
  QueueClosed(String)
  LeaseNotFound(String)
  BackendFailure(String)
  StateInvariant(String)
} derive(Eq, ToJson, Debug)
