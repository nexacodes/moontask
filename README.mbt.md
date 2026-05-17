# moontask

[English](README.mbt.md) | [简体中文](README.zh-CN.mbt.md)

`moontask` is a task queue library written in MoonBit for asynchronous job
processing scenarios that need a clean separation between
`producer / consumer / backend`.

The project currently provides:

- Strongly typed task definitions with payload encoding/decoding via `Codec[A]`
- `Producer` for enqueueing and `ConsumerService` for resident consumption
- A unified `QueueBackend` abstraction for swapping backend implementations
- A built-in `memory` backend for local development and testing
- A built-in `redis` backend for multi-process and distributed consumption
- Retry, delayed/scheduled delivery, dedupe keys, leases, heartbeats, and reaping
- Admin APIs for queue stats, job lookup, dead job retry, job cancellation, and queue pause/resume

This repository is inspired by, and partially behaviorally aligned with,
[`vmihailenco/taskq`](https://github.com/vmihailenco/taskq), while providing a
MoonBit-native implementation of the task model and runtime.

## Package Layout

- `nexacodes/moontask/core`: core types, task definitions, retry policies, and errors
- `nexacodes/moontask/registry`: task registry
- `nexacodes/moontask/backend`: backend trait
- `nexacodes/moontask/producer`: enqueue API
- `nexacodes/moontask/consumer`: resident consumer runtime
- `nexacodes/moontask/admin`: queue and consumer admin APIs
- `nexacodes/moontask/backends/memory`: in-memory backend
- `nexacodes/moontask/backends/redis`: Redis backend

## Use Cases

- Decoupling request handling from asynchronous processing
- Running multiple consumers concurrently
- Retrying failures, handling dead letters, and deduplicating jobs
- Developing locally with the memory backend, then deploying on Redis

## Installation

Add the dependency in `moon.mod.json`:

```json
{
  "deps": {
    "moonbitlang/async": "0.17.0",
    "nexacodes/moontask": "0.1.0"
  }
}
```

This project defaults to the `native` target.

## Quick Start

A minimal workflow usually has four steps:

1. Define the payload type and codec
2. Register the task in a `Registry`
3. Create a `Producer` and `ConsumerService`
4. Enqueue with `enqueue` and start the consumer runtime

Example:

```mbt
struct EmailPayload {
  to : String
  subject : String
  body : String
} derive(ToJson, FromJson, Debug)

fn email_codec() -> @core.Codec[EmailPayload] {
  {
    encode: fn(payload : EmailPayload) {
      @utf8.encode(payload.to_json().stringify())
    },
    decode: fn(data : Bytes) raise {
      let text = @utf8.decode(data)
      let json = @json.parse(text)
      @json.from_json(json)
    },
  }
}

async fn main {
  let backend = @memory.new_mem_backend()
  let registry = @registry.new_registry()

  let email_task = try! @registry.register_task(registry, {
    name: "email.send",
    codec: email_codec(),
    retry: @core.Fixed(max_attempts=3, delay_ms=500),
    handler: fn(_ctx : @core.TaskContext, payload : EmailPayload) {
      println("send email to \{payload.to}: \{payload.subject}")
    },
    fallback: None,
  })

  let producer = @producer.Producer::new(backend, "emails")
  let consumer = @consumer.ConsumerService::new(backend, registry, {
    ..@consumer.default_consumer_options("emails", "consumer-1"),
    max_concurrency: 4,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )

    ignore(
      try! producer.enqueue(email_task, {
        to: "alice@example.com",
        subject: "welcome",
        body: "hello from moontask",
      }),
    )

    @async.sleep(100)
    consumer.stop(1000)
    ignore(start_task.wait())
  })
}
```

For a more complete business-oriented example, see the demo directories.

## Examples

### 1. Local Memory Backend Demo

This demo has no external service dependency and is the fastest way to
understand the basic usage.

```bash
cd examples/order_email_demo
moon run src
```

Source:
[examples/order_email_demo/src/main.mbt](examples/order_email_demo/src/main.mbt)

### 2. Distributed Redis Demo

This demo shows how to:

- Use Redis as a durable backend
- Start multiple consumer processes for concurrent consumption
- Batch enqueue messages from a producer and poll for completion stats

Start Redis first:

```bash
docker run --rm --name moontask-redis -p 6379:6379 redis:7-alpine
```

Then enter the demo directory:

```bash
cd examples/redis_order_email_demo
```

Clear the demo keys:

```bash
moon run src/reset_app
```

Start two consumers:

```bash
moon run src/consumer_app -- -id consumer-1 -concurrency 8
moon run src/consumer_app -- -id consumer-2 -concurrency 8
```

Finally, start the producer:

```bash
moon run src/producer_app -- -count 1000 -timeout-ms 120000
```

Demo guide:
[examples/redis_order_email_demo/README.md](examples/redis_order_email_demo/README.md)

## Common APIs

- `@producer.Producer::enqueue`: enqueue with task defaults
- `@producer.Producer::enqueue_with_options`: enqueue with delay, dedupe, timeout, and retry overrides
- `@consumer.ConsumerService::start`: start the resident consumer loop
- `@consumer.ConsumerService::stop`: gracefully stop the consumer
- `@admin.AdminClient::queue_snapshot`: inspect queue stats
- `@admin.AdminClient::list_jobs`: list jobs with pagination
- `@admin.AdminClient::retry_dead_job`: retry a dead job
- `@admin.AdminClient::pause_queue` / `resume_queue`: pause or resume a queue

## Development

Common commands:

```bash
moon check
moon test
moon info
moon fmt
```

Notes:

- `moon info` updates `pkg.generated.mbti` for each package
- `moon fmt` formats MoonBit source files
- Redis-related tests require a locally reachable Redis instance at `127.0.0.1:6379`

## Status

The project already has usable core capabilities, including:

- Contract tests for the memory backend
- Contract tests for the Redis backend
- Behavioral tests for producer / consumer / admin
- A local demo and a Redis demo

Good next directions for expansion would be:

- More production-grade backends
- Better monitoring and tracing integration
- Finer-grained scheduling and orchestration capabilities

## License

This repository is licensed under Apache-2.0.

It also includes some third-party derived material based on
`vmihailenco/taskq`, which remains under BSD-2-Clause.

See:

- [LICENSE](LICENSE)
- [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)
- [licenses/taskq-BSD-2-Clause.txt](licenses/taskq-BSD-2-Clause.txt)
