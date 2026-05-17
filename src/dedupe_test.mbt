///|
test "once_in_period generates a stable bucketed dedupe key" {
  let first = @moontask.once_in_period(
    "email.send", "user-42", 60_000L, 120_001L,
  )
  let second = @moontask.once_in_period(
    "email.send", "user-42", 60_000L, 179_999L,
  )
  let next_bucket = @moontask.once_in_period(
    "email.send", "user-42", 60_000L, 180_000L,
  )
  assert_eq(first, second)
  assert_true(first != next_bucket)
}
