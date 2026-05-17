///|
let help_message : String =
  #|Usage: moon run src/producer_app -- [OPTIONS]
  #|
  #|Options:
  #|  -count N         enqueue N messages, default 1000
  #|  -timeout-ms N    wait at most N milliseconds for consumers, default 120000
  #|  -h, --help       show this message

///|
async fn main {
  let mut total_messages = 1000
  let mut timeout_ms = 120_000
  for rest = @env.args()[1:] {
    match rest {
      ["-count", .. rest] => {
        guard rest is [value, .. rest] else {
          fail("expected integer after `-count`")
        }
        total_messages = @string.parse_int(value)
        continue rest
      }
      ["-timeout-ms", .. rest] => {
        guard rest is [value, .. rest] else {
          fail("expected integer after `-timeout-ms`")
        }
        timeout_ms = @string.parse_int(value)
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

  guard total_messages > 0 else { fail("`-count` must be positive") }
  guard timeout_ms > 0 else { fail("`-timeout-ms` must be positive") }

  let backend = @demo.new_demo_backend()
  let admin = @admin.AdminClient::new(backend)
  let health = try! admin.health()
  println("redis health: \{health.to_json().stringify()}")

  let registry = @registry.new_registry()
  let task = try! @demo.register_producer_task(registry)
  let producer = @producer.Producer::new(backend, @demo.queue_name)

  println(
    "enqueueing \{total_messages} messages to queue=\{@demo.queue_name} prefix=\{@demo.redis_prefix}",
  )
  let started_at = @async.now()
  for sequence_no in 1..<=total_messages {
    let payload = @demo.build_payload(sequence_no)
    ignore(
      producer.enqueue_with_options(task, payload, {
        ..@core.default_enqueue_options(),
        timeout_ms: Some(10_000),
      }),
    )
    if sequence_no % 100 == 0 || sequence_no == total_messages {
      println("enqueued \{sequence_no}/\{total_messages}")
    }
  }
  let enqueue_elapsed_ms = @async.now() - started_at
  println("enqueue completed in \{enqueue_elapsed_ms} ms")

  let final_stats = try! @demo.wait_until_processed(
    backend, total_messages, timeout_ms,
  )
  println("final stats: \{final_stats.to_json().stringify()}")

  if final_stats.succeeded >= total_messages {
    println("all messages processed successfully")
  } else if final_stats.dead > 0 {
    fail("some messages entered dead letter state")
  } else {
    fail("timed out before all messages were processed")
  }
}
