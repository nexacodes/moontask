///|
/// Entry point for the CLI or runnable program in this template.
/// Keep main focused; move logic into the library package.
/// Run with `moon run cmd/main` from the project root.
fn main {
  let registry = @taskq.new_registry()
  let ping_task = try! @taskq.register_task(registry, {
    name: "phase-one.ping",
    codec: @taskq.bytes_codec(),
    retry: @taskq.NoRetry,
    handler: fn(_context : @taskq.TaskContext, _payload : Bytes) { () },
    fallback: None,
  })
  let queue = @mem.MemQueue::new(
    registry,
    @taskq.default_queue_options("phase-one-demo"),
  )
  let job_id = try! queue.enqueue(
    ping_task,
    b"hello, phase one",
    @taskq.default_enqueue_options(),
  )
  println("phase one memqueue skeleton")
  println(job_id)
  println(queue.queue_name())
  println(queue.job_count())
}
