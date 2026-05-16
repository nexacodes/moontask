///|
test "register_task stores phase-one registry metadata" {
  let registry = @moontask.new_registry()
  let task = try! @moontask.register_task(registry, {
    name: "spec.register",
    codec: @moontask.bytes_codec(),
    retry: @moontask.Fixed(max_attempts=3, delay_ms=250),
    handler: fn(_context : @moontask.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  assert_eq(task.name, "spec.register")
  assert_eq(task.id, 1)
  assert_true(registry.contains("spec.register"))
  assert_eq(registry.task_count(), 1)
}
