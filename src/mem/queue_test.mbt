///|
async test "mem queue drains ready jobs without losing them" {
  let handled = []
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.process-all",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, payload : Bytes) {
      handled.push(payload)
    },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-two-mem"),
  )
  let first_job_id = try! queue.enqueue(
    task,
    b"first",
    @taskq.default_enqueue_options(),
  )
  let second_job_id = try! queue.enqueue(
    task,
    b"second",
    @taskq.default_enqueue_options(),
  )
  assert_eq(first_job_id, "phase-two-mem-job-1")
  assert_eq(second_job_id, "phase-two-mem-job-2")
  assert_eq(queue.job_count(), 2)
  assert_eq(queue.ready_job_count(), 2)

  let processed_count = queue.process_all()
  assert_eq(processed_count, 2)
  assert_eq(handled.length(), 2)
  assert_eq(handled[0], b"first")
  assert_eq(handled[1], b"second")
  assert_eq(queue.job_count(), 0)
  assert_eq(queue.ready_job_count(), 0)
  assert_eq(queue.inflight_job_count(), 0)

  let snapshot = queue.snapshot()
  assert_eq(snapshot.processed, 2)
  assert_eq(snapshot.succeeded, 2)
  assert_eq(snapshot.failed, 0)
  assert_eq(snapshot.queued, 0)
}

///|
async test "graceful shutdown waits for inflight worker completion" {
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "mem.shutdown",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-two-shutdown"),
  )
  let job_id = try! queue.enqueue(
    task,
    b"payload",
    @taskq.default_enqueue_options(),
  )
  let lease = match queue.dispatch_next() {
    Some(lease) => lease
    None => fail("expected ready job to dispatch")
  }
  assert_eq(lease.job.id, job_id)
  assert_eq(queue.ready_job_count(), 0)
  assert_eq(queue.inflight_job_count(), 1)

  queue.shutdown_gracefully()
  assert_true(queue.lifecycle() is @runtime.QueueLifecycle::Closing)
  assert_eq(queue.inflight_job_count(), 1)

  let enqueue_after_shutdown : Result[@taskq.JobId, Error] = try? queue.enqueue(
    task,
    b"payload-2",
    @taskq.default_enqueue_options(),
  )
  guard enqueue_after_shutdown
    is Err(@taskq.QueueError::QueueClosed(queue_name)) &&
    queue_name == "phase-two-shutdown" else {
    fail("expected queue to reject new jobs while closing")
  }

  queue.complete_success(lease)
  assert_true(queue.lifecycle() is @runtime.QueueLifecycle::Closed)
  assert_eq(queue.inflight_job_count(), 0)
  assert_eq(queue.job_count(), 0)

  let snapshot = queue.snapshot()
  assert_eq(snapshot.processed, 1)
  assert_eq(snapshot.succeeded, 1)
}
