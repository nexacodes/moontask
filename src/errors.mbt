///|
pub(all) suberror QueueError {
  DuplicateTask(String)
  TaskNotFound(String)
  QueueClosed(String)
  InvalidQueueOptions(String)
  StateInvariant(String)
} derive(Eq, ToJson, Debug)
