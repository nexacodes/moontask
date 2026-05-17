///|
struct MarkHook {
  name : String
  marks : Array[String]
  events : Array[@taskq.ProcessEvent]
  results : Array[@taskq.ProcessResult]
}

///|
impl @taskq.Hook for MarkHook with before_process(self, event) {
  self.marks.push("\{self.name}.before")
  self.events.push(event)
}

///|
impl @taskq.Hook for MarkHook with after_process(self, _event, result) {
  self.marks.push("\{self.name}.after")
  self.results.push(result)
}

///|
struct FailingHook {
  marks : Array[String]
}

///|
impl @taskq.Hook for FailingHook with before_process(self, _event) raise {
  self.marks.push("failing.before")
  raise @taskq.QueueError::StateInvariant("hook before failed")
}

///|
impl @taskq.Hook for FailingHook with after_process(self, _event, _result) raise {
  self.marks.push("failing.after")
  raise @taskq.QueueError::StateInvariant("hook after failed")
}

///|
async test "hooks run in registration order for successful attempts" {
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.m5.hook.success",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let fake_clock = @testing.new_fake_clock(1000)
  let marks = []
  let first_hook = MarkHook::{ name: "first", marks, events: [], results: [] }
  let second_hook = MarkHook::{ name: "second", marks, events: [], results: [] }
  let queue = @mem.MemQueue::new_with_runtime(
    registry,
    @taskq.default_queue_options("phase-five-hook-success"),
    { hooks: [first_hook, second_hook], clock: fake_clock.runtime_clock() },
  )
  ignore(try! queue.enqueue(task, b"payload", @taskq.default_enqueue_options()))

  @test.assert_eq(queue.process_all(), 1)
  @test.assert_eq(marks, [
    "first.before", "second.before", "first.after", "second.after",
  ])
  @test.assert_eq(first_hook.events.length(), 1)
  @test.assert_eq(first_hook.events[0].queue_name, "phase-five-hook-success")
  @test.assert_eq(first_hook.events[0].task_name, "mem.m5.hook.success")
  @test.assert_eq(first_hook.events[0].attempt, 1)
  @test.assert_eq(first_hook.events[0].max_attempts, 1)
  @test.assert_eq(first_hook.events[0].enqueued_at_ms, 1000L)
  @test.assert_eq(first_hook.events[0].visible_at_ms, 1000L)
  @test.assert_eq(first_hook.events[0].started_at_ms, 1000L)
  @test.assert_eq(first_hook.results.length(), 1)
  assert_true(first_hook.results[0].outcome is @taskq.Succeeded)
  @test.assert_eq(first_hook.results[0].error_message, None)
  @test.assert_eq(first_hook.results[0].duration_ms, 0L)
  @test.assert_eq(first_hook.results[0].finished_at_ms, 1000L)
  @test.assert_eq(queue.hook_error_count(), 0)
}

///|
async test "hook failures do not change retry and dead transitions" {
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.m5.hook.failure",
    codec: @taskq.bytes_codec(),
    retry: @taskq.Fixed(max_attempts=2, delay_ms=50),
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) raise {
      raise @taskq.QueueError::StateInvariant("still failing")
    },
    fallback: None,
  })
  let fake_clock = @testing.new_fake_clock(200)
  let marks = []
  let failing_hook = FailingHook::{ marks, }
  let recording_hook = MarkHook::{
    name: "recording",
    marks,
    events: [],
    results: [],
  }
  let queue = @mem.MemQueue::new_with_runtime(
    registry,
    { ..@taskq.default_queue_options("phase-five-hook-failure") },
    { hooks: [failing_hook, recording_hook], clock: fake_clock.runtime_clock() },
  )
  ignore(try! queue.enqueue(task, b"payload", @taskq.default_enqueue_options()))

  @test.assert_eq(queue.process_all(), 2)
  @test.assert_eq(marks, [
    "failing.before", "recording.before", "failing.after", "recording.after", "failing.before",
    "recording.before", "failing.after", "recording.after",
  ])
  @test.assert_eq(recording_hook.results.length(), 2)
  assert_true(recording_hook.results[0].outcome is @taskq.FailedWillRetry)
  @test.assert_eq(recording_hook.results[0].next_visible_at_ms, Some(250L))
  assert_true(recording_hook.results[1].outcome is @taskq.FailedDead)
  @test.assert_eq(recording_hook.results[1].next_visible_at_ms, None)
  @test.assert_eq(queue.hook_error_count(), 4)
  @test.assert_eq(fake_clock.sleep_history(), [50])

  let snapshot = queue.snapshot()
  @test.assert_eq(snapshot.processed, 2)
  @test.assert_eq(snapshot.retried, 1)
  @test.assert_eq(snapshot.failed, 1)
  @test.assert_eq(snapshot.dead, 1)
}

///|
async test "admin snapshot exposes ready delayed inflight and dedupe state" {
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.m5.admin",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let fake_clock = @testing.new_fake_clock(500)
  let queue = @mem.MemQueue::new_with_runtime(
    registry,
    @taskq.default_queue_options("phase-five-admin"),
    { hooks: [], clock: fake_clock.runtime_clock() },
  )
  ignore(
    try! queue.enqueue(task, b"first", {
      ..@taskq.default_enqueue_options(),
      dedupe_key: Some("ready-key"),
    }),
  )
  ignore(
    try! queue.enqueue(task, b"second", {
      ..@taskq.default_enqueue_options(),
      dedupe_key: Some("delay-key"),
      delay_ms: 30,
    }),
  )
  ignore(
    try! queue.enqueue(task, b"third", {
      ..@taskq.default_enqueue_options(),
      dedupe_key: Some("third-key"),
    }),
  )
  let lease = match queue.dispatch_next() {
    Some(lease) => lease
    None => fail("expected first ready job to dispatch")
  }
  @test.assert_eq(lease.job.id, "phase-five-admin-job-1")

  let admin = queue.admin_snapshot()
  assert_true(admin.lifecycle is @runtime.QueueLifecycle::Running)
  assert_false(admin.paused)
  @test.assert_eq(admin.next_job_id, 4)
  @test.assert_eq(admin.consecutive_failures, 0)
  @test.assert_eq(admin.ready_job_ids, ["phase-five-admin-job-3"])
  @test.assert_eq(admin.delayed_job_ids, ["phase-five-admin-job-2"])
  @test.assert_eq(admin.inflight_job_ids, ["phase-five-admin-job-1"])
  @test.assert_eq(admin.dedupe_keys, ["delay-key", "ready-key", "third-key"])
  @test.assert_eq(admin.hook_errors, 0)
  @test.assert_eq(queue.ready_job_count(), 1)
  @test.assert_eq(queue.delayed_job_count(), 1)
  @test.assert_eq(queue.inflight_job_count(), 1)
  @test.assert_eq(queue.dedupe_job_count(), 3)
}

///|
async test "fake clock drives delayed jobs without wall time sleeps" {
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.m5.fake-clock",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let fake_clock = @testing.new_fake_clock(1000)
  let queue = @mem.MemQueue::new_with_runtime(
    registry,
    @taskq.default_queue_options("phase-five-fake-clock"),
    { hooks: [], clock: fake_clock.runtime_clock() },
  )
  ignore(
    try! queue.enqueue(task, b"payload", {
      ..@taskq.default_enqueue_options(),
      delay_ms: 40,
    }),
  )

  @test.assert_eq(queue.process_all(), 1)
  @test.assert_eq(fake_clock.sleep_history(), [40])
  @test.assert_eq(fake_clock.now_ms(), 1040L)
  @test.assert_eq(queue.snapshot().succeeded, 1)
  @test.assert_eq(queue.snapshot().delayed, 0)
}
