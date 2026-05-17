///|
async fn main {
  let job_count = 10000
  let registry = @taskq.new_registry()
  let task = try! @taskq.register_task(registry, {
    name: "benchmark.noop",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(registry, {
    queue_name: "benchmark-memqueue",
    max_concurrency: 64,
    pause_after_failures: 0,
    rate_limit_per_second: None,
  })
  for index in 0..<job_count {
    ignore(index)
    ignore(
      try! queue.enqueue(task, b"payload", @taskq.default_enqueue_options()),
    )
  }

  let started_at_ms = @async.now()
  let processed = queue.process_all()
  let elapsed_ms = Int::max(1, (@async.now() - started_at_ms).to_int())
  let throughput = processed * 1000 / elapsed_ms

  println("benchmark=memqueue-noop")
  println("jobs=\{job_count}")
  println("processed=\{processed}")
  println("elapsed_ms=\{elapsed_ms}")
  println("throughput_jobs_per_s=\{throughput}")
}
