///|
fn retry_delay_ms(policy : @core.RetryPolicy, attempt : Int) -> Int? {
  match policy {
    @core.NoRetry => None
    @core.Fixed(delay_ms~, ..) => Some(delay_ms)
    @core.Exponential(initial_delay_ms~, factor~, max_delay_ms~, ..) => {
      let exponent = Double::from_int(attempt - 1)
      let scaled = initial_delay_ms.to_double() * @math.pow(factor, exponent)
      Some(Int::min(scaled.to_int(), max_delay_ms))
    }
  }
}

///|
fn next_retry_visible_at_ms(
  clock : @core.RuntimeClock,
  lease : @core.Lease,
  policy : @core.RetryPolicy,
) -> Int64? {
  match retry_delay_ms(policy, lease.job.attempt) {
    Some(delay_ms) => Some((clock.now_ms)() + Int64::from_int(delay_ms))
    None => None
  }
}
