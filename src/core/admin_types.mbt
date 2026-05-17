///|
pub(all) struct JobPage {
  jobs : Array[JobRecord]
  next_cursor : String?
} derive(Eq, ToJson, Debug)

///|
pub(all) struct BackendHealth {
  ok : Bool
  backend_kind : String
  detail : String?
} derive(Eq, ToJson, Debug)
