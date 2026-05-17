///|
/// One page of jobs returned by admin listing APIs.
pub(all) struct JobPage {
  jobs : Array[JobRecord]
  next_cursor : String?
} derive(Eq, ToJson, Debug)

///|
/// Health status reported by a backend implementation.
pub(all) struct BackendHealth {
  ok : Bool
  backend_kind : String
  detail : String?
} derive(Eq, ToJson, Debug)
