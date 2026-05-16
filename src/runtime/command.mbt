///|
pub(all) enum Command {
  Enqueue(@taskq.JobEnvelope)
  Dispatch
  CompleteSuccess(String)
  CompleteFailure(String, String)
  PollStats
  Shutdown(Bool, Int)
  Pause(String)
  Resume
  Cancel(String, String)
} derive(Eq, ToJson, Debug)
