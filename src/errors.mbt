///|
pub(all) suberror QueueError {
  DuplicateTask(String)
  DuplicateJob(String)
  TaskNotFound(String)
  QueueClosed(String)
  InvalidQueueOptions(String)
  StateInvariant(String)
} derive(Eq, ToJson, Debug)
