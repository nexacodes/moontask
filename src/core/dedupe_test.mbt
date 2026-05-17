///|
test "once_in_period keeps stable bucket within the same period" {
  let first = once_in_period("email.send", "order-1", 1000L, 1999L)
  let second = once_in_period("email.send", "order-1", 1000L, 1001L)
  let third = once_in_period("email.send", "order-1", 1000L, 2000L)

  @test.assert_eq(first, second)
  assert_true(first != third)
}
