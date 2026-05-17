///|
pub fn once_in_period(
  task_name : String,
  business_key : String,
  period_ms : Int64,
  now_ms : Int64,
) -> String {
  let bucket = if period_ms > Int64::default() {
    now_ms / period_ms
  } else {
    Int64::default()
  }
  "\{task_name}:\{business_key}:\{bucket}"
}
