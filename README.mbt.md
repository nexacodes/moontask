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

Most day-to-day APIs are also re-exported from the root package
`nexacodes/moontask`, so common applications usually only need one main library
import, plus `nexacodes/moontask/core` when they want raw retry-policy or error
constructors.

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
    "nexacodes/moontask": "0.1.2"
  }
}
```

This project defaults to the `native` target.

Typical application import:

```mbt
import {
  "moonbitlang/async",
  "moonbitlang/core/encoding/utf8" @utf8,
  "moonbitlang/core/json" @json,
  "nexacodes/moontask" @moontask,
  "nexacodes/moontask/core" @core,
}
```

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

fn email_codec() -> @moontask.Codec[EmailPayload] {
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
  let backend = @moontask.new_mem_backend()
  let registry = @moontask.new_registry()

  let email_task = try! @moontask.register_task(registry, {
    name: "email.send",
    codec: email_codec(),
    retry: @core.Fixed(max_attempts=3, delay_ms=500),
    handler: fn(_ctx : @moontask.TaskContext, payload : EmailPayload) {
      println("send email to \{payload.to}: \{payload.subject}")
    },
    fallback: None,
  })

  let producer = @moontask.Producer::new(backend, "emails")
  let consumer = @moontask.ConsumerService::new(backend, registry, {
    ..@moontask.default_consumer_options("emails", "consumer-1"),
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

### Root Package Facade

The root package re-exports the most commonly used APIs, including:

- task types and defaults from `core`
- `Registry` and `register_task`
- `Producer` and `ConsumerService`
- `AdminClient`
- `new_mem_backend` and `new_redis_backend`

Lower-level or more specialized packages are still available when needed.
`nexacodes/moontask/core` remains useful when you want raw enum/error
constructors such as `@core.Fixed(...)`.

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

- `@moontask.Producer::enqueue`: enqueue with task defaults
- `@moontask.Producer::enqueue_with_options`: enqueue with delay, dedupe, timeout, and retry overrides
- `@moontask.ConsumerService::start`: start the resident consumer loop
- `@moontask.ConsumerService::stop`: gracefully stop the consumer
- `@moontask.AdminClient::queue_snapshot`: inspect queue stats
- `@moontask.AdminClient::list_jobs`: list jobs with pagination
- `@moontask.AdminClient::retry_dead_job`: retry a dead job
- `@moontask.AdminClient::pause_queue` / `resume_queue`: pause or resume a queue

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
