///|
let help_message : String =
  #|Usage: moon run src/consumer_app -- [OPTIONS]
  #|
  #|Options:
  #|  -id NAME           consumer id, default consumer-1
  #|  -concurrency N     max concurrency, default 8
  #|  -batch N           reserve batch size, default 16
  #|  -stats-ms N        metrics print interval in milliseconds, default 1000
  #|  -h, --help         show this message

///|
async fn main {
  let mut consumer_id = "consumer-1"
  let mut max_concurrency = 8
  let mut reserve_batch_size = 16
  let mut stats_interval_ms = 1000
  for rest = @env.args()[1:] {
    match rest {
      ["-id", .. rest] => {
        guard rest is [value, .. rest] else {
          fail("expected consumer id after `-id`")
        }
        consumer_id = value
        continue rest
      }
      ["-concurrency", .. rest] => {
        guard rest is [value, .. rest] else {
          fail("expected integer after `-concurrency`")
        }
        max_concurrency = @string.parse_int(value)
        continue rest
      }
      ["-batch", .. rest] => {
        guard rest is [value, .. rest] else {
          fail("expected integer after `-batch`")
        }
        reserve_batch_size = @string.parse_int(value)
        continue rest
      }
      ["-stats-ms", .. rest] => {
        guard rest is [value, .. rest] else {
          fail("expected integer after `-stats-ms`")
        }
        stats_interval_ms = @string.parse_int(value)
        continue rest
      }
      ["-h" | "--help", ..] => {
        println(help_message)
        return
      }
      [['-', ..] as opt, ..] => fail("unknown option `\{opt}`")
      [arg, ..] => fail("unexpected positional argument `\{arg}`")
      [] => break
    }
  }

  guard consumer_id != "" else { fail("`-id` must not be empty") }
  guard max_concurrency > 0 else { fail("`-concurrency` must be positive") }
  guard reserve_batch_size > 0 else { fail("`-batch` must be positive") }
  guard stats_interval_ms > 0 else { fail("`-stats-ms` must be positive") }

  let backend = @demo.new_demo_backend()
  let registry = @moontask.new_registry()
  ignore(try! @demo.register_consumer_task(registry, consumer_id))
  let consumer = @moontask.ConsumerService::new(backend, registry, {
    ..@moontask.default_consumer_options(@demo.queue_name, consumer_id),
    max_concurrency,
    reserve_batch_size,
    lease_ms: 10_000,
    poll_interval_ms: 20,
    pause_poll_interval_ms: 100,
    heartbeat_interval_ms: 200,
    reaper_interval_ms: 200,
  })
  let admin = @moontask.AdminClient::new(backend)

  println(
    "consumer starting id=\{consumer_id} queue=\{@demo.queue_name} concurrency=\{max_concurrency} batch=\{reserve_batch_size}",
  )
  @async.with_task_group(async fn(group) {
    ignore(group.spawn(async fn() { try! consumer.start() }, no_wait=true))
    while true {
      @async.sleep(stats_interval_ms)
      let metrics = admin.metrics(consumer)
      println(
        "[\{consumer_id}] processed=\{metrics.processed_attempts} local_success=\{metrics.succeeded_attempts} queue_pending=\{metrics.pending} queue_running=\{metrics.running} queue_succeeded=\{metrics.succeeded} queue_dead=\{metrics.dead}",
      )
    }
  })
}
