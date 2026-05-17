///|
pub(all) enum Command {
  Enqueue(job~ : @taskq.JobEnvelope)
  Dispatch
  Ack(job_id~ : String)
  Nack(job_id~ : String, error_message~ : String)
  PollStats
  Shutdown(graceful~ : Bool, timeout_ms~ : Int)
  Pause(reason~ : String)
  Resume
  Cancel(job_id~ : String, reason~ : String)
} derive(Eq, ToJson, Debug)
