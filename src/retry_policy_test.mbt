///|
test "retry policy constructors preserve labeled fields" {
  let fixed = @moontask.Fixed(max_attempts=3, delay_ms=250)
  assert_eq(@moontask.retry_policy_max_attempts(fixed), 3)
  assert_true(fixed is @moontask.Fixed(max_attempts=3, delay_ms=250))

  let exponential = @moontask.Exponential(
    max_attempts=5,
    initial_delay_ms=100,
    factor=2.0,
    max_delay_ms=5_000,
  )
  assert_eq(@moontask.retry_policy_max_attempts(exponential), 5)
  assert_true(
    exponential
    is @moontask.Exponential(
      max_attempts=5,
      initial_delay_ms=100,
      factor=2.0,
      max_delay_ms=5_000
    ),
  )
}
