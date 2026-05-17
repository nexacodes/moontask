# moontask

[English](README.mbt.md) | 简体中文

`moontask` 是一个用 MoonBit 编写的任务队列库，面向需要
`producer / consumer / backend` 解耦的异步任务处理场景。

项目当前提供：

- 强类型任务定义，任务载荷通过 `Codec[A]` 编解码
- `Producer` 负责入队，`ConsumerService` 负责常驻消费
- 统一的 `QueueBackend` 抽象，便于切换不同后端
- 内置 `memory` 后端，适合本地开发和测试
- 内置 `redis` 后端，适合多进程/分布式消费
- 重试、延迟/定时投递、去重键、lease、心跳续租、过期回收
- 管理接口：队列统计、任务查询、重试 dead job、取消任务、暂停/恢复队列

这个仓库的设计和部分行为参考了
[`vmihailenco/taskq`](https://github.com/vmihailenco/taskq)，并在 MoonBit
生态中实现了对应的任务模型和运行时。

## 包结构

- `nexacodes/moontask/core`: 核心类型、任务定义、重试策略、错误类型
- `nexacodes/moontask/registry`: 任务注册中心
- `nexacodes/moontask/backend`: 后端 trait
- `nexacodes/moontask/producer`: 入队 API
- `nexacodes/moontask/consumer`: 常驻 consumer 运行时
- `nexacodes/moontask/admin`: 队列与 consumer 管理接口
- `nexacodes/moontask/backends/memory`: 内存后端
- `nexacodes/moontask/backends/redis`: Redis 后端

## 适用场景

- 需要把业务请求和异步处理解耦
- 需要多 consumer 并发处理任务
- 需要失败重试、dead letter、任务去重
- 需要在本地先用内存后端开发，再切到 Redis 后端部署

## 安装

在 `moon.mod.json` 中加入依赖：

```json
{
  "deps": {
    "moonbitlang/async": "0.17.0",
    "nexacodes/moontask": "0.1.0"
  }
}
```

本项目默认使用 `native` target。

## 快速使用

一个最小流程通常分成四步：

1. 定义 payload 和 codec
2. 在 `Registry` 中注册任务
3. 创建 `Producer` 和 `ConsumerService`
4. 调用 `enqueue` 入队，并启动 consumer 常驻消费

示例：

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

如果你需要更完整的业务代码，直接看示例目录。

## 示例工程

### 1. 本地内存后端示例

这个示例不依赖外部服务，适合快速理解基本用法。

```bash
cd examples/order_email_demo
moon run src
```

示例位置：
[examples/order_email_demo/src/main.mbt](examples/order_email_demo/src/main.mbt)

### 2. Redis 分布式示例

这个示例演示：

- 使用 Redis 作为 durable backend
- 启动多个 consumer 进程并发消费
- producer 批量发送消息并轮询统计结果

先启动 Redis：

```bash
docker run --rm --name moontask-redis -p 6379:6379 redis:7-alpine
```

然后进入示例目录：

```bash
cd examples/redis_order_email_demo
```

清理示例 key：

```bash
moon run src/reset_app
```

启动两个 consumer：

```bash
moon run src/consumer_app -- -id consumer-1 -concurrency 8
moon run src/consumer_app -- -id consumer-2 -concurrency 8
```

最后启动 producer：

```bash
moon run src/producer_app -- -count 1000 -timeout-ms 120000
```

示例说明：
[examples/redis_order_email_demo/README.md](examples/redis_order_email_demo/README.md)

## 常用 API

- `@producer.Producer::enqueue`: 按任务默认配置入队
- `@producer.Producer::enqueue_with_options`: 指定延迟、去重、超时、最大重试次数
- `@consumer.ConsumerService::start`: 启动 consumer 常驻循环
- `@consumer.ConsumerService::stop`: 优雅停止 consumer
- `@admin.AdminClient::queue_snapshot`: 查看队列统计
- `@admin.AdminClient::list_jobs`: 分页列出任务
- `@admin.AdminClient::retry_dead_job`: 重试 dead job
- `@admin.AdminClient::pause_queue` / `resume_queue`: 暂停或恢复队列

## 开发

常用命令：

```bash
moon check
moon test
moon info
moon fmt
```

说明：

- `moon info` 会更新各包的 `pkg.generated.mbti`
- `moon fmt` 用于格式化 MoonBit 源码
- Redis 相关测试依赖本地 `127.0.0.1:6379` 可访问的 Redis 实例

## 状态

项目已经具备可用的核心能力，包括：

- memory backend 的契约测试
- redis backend 的契约测试
- producer / consumer / admin 的行为测试
- 本地 demo 和 Redis demo

后续如果继续扩展，可以优先考虑：

- 更多生产级 backend
- 更完整的监控与 tracing 集成
- 更细粒度的调度与任务编排能力

## License

本仓库使用 Apache-2.0 许可证。

另外，仓库包含一部分基于 `vmihailenco/taskq` 衍生的第三方材料，这部分遵循
BSD-2-Clause。

详见：

- [LICENSE](LICENSE)
- [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)
- [licenses/taskq-BSD-2-Clause.txt](licenses/taskq-BSD-2-Clause.txt)
