///|
test "register_task assigns stable ids and stores task names" {
  let registry = new_registry()
  let task = try! register_task(registry, {
    name: "email.send",
    codec: @core.bytes_codec(),
    retry: @core.NoRetry,
    handler: fn(_context : @core.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })

  @test.assert_eq(task.id, 1)
  @test.assert_eq(task.name, "email.send")
  assert_true(registry.contains("email.send"))
  @test.assert_eq(registry.task_count(), 1)
  @test.assert_eq(registry.task_names(), ["email.send"])
}
