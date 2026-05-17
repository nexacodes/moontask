///|
test "retry policy max attempts matches constructor labels" {
  @test.assert_eq(retry_policy_max_attempts(NoRetry), 1)
  @test.assert_eq(
    retry_policy_max_attempts(Fixed(max_attempts=3, delay_ms=250)),
    3,
  )
  @test.assert_eq(
    retry_policy_max_attempts(
      Exponential(
        max_attempts=5,
        initial_delay_ms=100,
        factor=2.0,
        max_delay_ms=1600,
      ),
    ),
    5,
  )
}
