# MoonBit 异步任务队列实现方案

本文目标是参考 Go 的 `vmihailenco/taskq`，在 MoonBit 中设计一个“生产可用但当前仅支持内存后端”的异步任务队列。这里的“生产可用”指：

- 单进程内具备稳定的投递、消费、重试、延迟调度、限流、优雅关闭和观测能力。
- 明确至少一次投递语义和失败恢复路径。
- API 为未来扩展 Redis / SQS / 持久化后端预留抽象。

同时必须明确当前阶段的边界：

- `memqueue` 版本不具备跨进程持久化。
- 进程崩溃或重启后，内存中的待执行任务会丢失。
- “生产级”只成立于单进程运行时工程质量，而不是 durable queue。

## 1. `taskq` 的架构要点

`taskq` 的核心设计不是“某个具体队列实现”，而是把任务系统拆成三层：

1. 任务定义层
   `Task` / `TaskOptions` 定义任务名、处理函数、重试策略、回退处理器。
2. 消费运行时层
   `Consumer` 负责取消息、执行 handler、统计、重试、失败暂停、hook。
3. 队列后端层
   `Queue` 接口抽象具体存储与投递后端，Redis/SQS/IronMQ/memqueue 都挂在这一层。

这个分层很值得保留，因为它把“业务任务定义”和“消息存储介质”分开了。

### 1.1 `taskq` 的核心对象

从 `pkg.go.dev` 和源码可以归纳出几个关键对象：

- `TaskOptions`
  包含 `Name`、`Handler`、`FallbackHandler`、`RetryLimit`、`MinBackoff`、`MaxBackoff`。
- `Message`
  是任务实例。关键字段包括 `ID`、`Name`、`Delay`、`Args`、`TaskName`、`ReservedCount`、`Err`。
- `QueueOptions`
  包含并发度、reservation、buffer、pause 阈值、rate limit、storage、handler 等。
- `Queue`
  抽象 `Add / ReserveN / Release / Delete / Purge / Close`。
- `QueueConsumer`
  抽象消费生命周期与统计：`Start / Stop / Process / ProcessAll / Stats`。

这说明 `taskq` 的设计中心是“消息 envelope + 消费状态机”，而不是只暴露一个 `enqueue(fn)`。

### 1.2 `taskq` 的关键运行语义

`taskq` 的使用体验看起来简单，但背后有几条重要语义：

- 至少一次投递
  消费失败后消息不会立刻丢弃，而是 release/requeue。
- 延迟任务
  `Message.Delay` 支持未来时刻执行。
- 去重投递
  `Message.Name` 可用于 once/dedup；`OnceInPeriod` 会按 period + args 生成去重 key。
- 指数退避
  失败后按重试次数推导下一次执行时间。
- 回退处理
  超过重试上限后交给 `FallbackHandler`。
- 错误风暴暂停
  连续失败达到阈值后，消费者会 pause，避免无限空转。
- 运行时统计
  `ConsumerStats` 提供 worker/fetcher/buffer/inflight/retries/fails/timing 等指标。

这些能力里，真正值得在 MoonBit 第一版里保留的是：

- 任务注册
- 消息 envelope
- 延迟/重试调度
- 并发消费控制
- 去重
- 失败回退
- 统计和 hook
- 优雅关闭

### 1.3 `memqueue` 给出的启发

`taskq` 的 `memqueue` 并没有完整实现远端队列那套 reservation 协议，而是做了一个更轻量的本地版本：

- `Add` 时直接进入本地消费运行时。
- 延迟任务用 timer 调度。
- `Release` 通过克隆消息重新入队。
- `Delete` 用于完成消息并减少待处理计数。
- 去重通过本地 `Storage.Exists(...)` 判断。

这个点非常重要：即使在 Go 里，`taskq` 也没有为了保持所有后端“完全一致”而强行把内存队列做复杂。MoonBit 版本也应该采用相同策略：

- 公共抽象保留未来可扩展性。
- 内存后端按单进程最佳实现来做，不机械模拟远端 reservation 协议。

## 2. MoonBit 实现时的关键差异

这里不能直接照搬 Go 版本，因为 MoonBit 的并发模型和类型系统不同。

### 2.1 并发模型差异

Go 依赖 goroutine + channel，`taskq` 的 worker/fetcher 自伸缩天然成立。

MoonBit 当前异步模型基于 `async` 和官方运行时 `moonbitlang/async`，而且官方文档明确说明：

- 需要显式引入 `moonbitlang/async`。
- `async` 当前对 `native` 后端支持最好。
- 并发管理以 `TaskGroup`、取消传播、超时、重试组合子为核心。

这意味着 MoonBit 版不应该设计成“很多独立 goroutine 随便共享状态”，而应该优先采用：

- 单 owner 的状态机 actor。
- 事件循环 + 任务组。
- 用 `Semaphore` 控制并发，而不是到处加锁。

### 2.2 反射能力差异

Go 版 `TaskOptions.Handler interface{}` 能接受多种签名，核心原因是它依赖反射适配调用。

MoonBit 不适合这样做。MoonBit 版应该改成两层 API：

1. 原始层：任务 payload 是 `Bytes` 或 `Json`。
2. 类型安全层：`Task[A]` 带 codec，自动完成 `A <-> payload`。

也就是说，不要试图复制 Go 的“任意 handler 签名”体验，而要用更稳健的显式 codec 模型。

### 2.3 运行时稳定性差异

MoonBit 官方文档说明 `moonbitlang/async` API 仍可能变化。因此实现时最好把运行时依赖隔离在内部适配层，不让公共 API 直接暴露太多 async runtime 细节。

建议做一个很薄的内部 runtime facade：

- `sleep_ms`
- `now_ms`
- `spawn_bg`
- `with_timeout`
- `Semaphore`

这样未来即使 `moonbitlang/async` API 变化，改动也主要局限在内部模块。

## 3. 推荐的 MoonBit 总体设计

## 3.1 设计目标

第一阶段只做单进程内存队列，但公共 API 要让未来增加持久化后端时不需要推倒重来。

我建议把系统拆成五个模块：

1. `core`
   公共类型：任务、消息、错误、重试策略、hook、统计。
2. `registry`
   任务注册表，管理 `task_name -> handler/codec`。
3. `runtime`
   消费执行器，负责 worker 并发、ack/nack、重试、fallback、pause。
4. `mem`
   内存后端，负责 ready queue / delayed heap / inflight map / dedupe index。
5. `testing`
   测试辅助和 fake clock。

如果按 MoonBit 包组织，建议目录如下：

```text
.
├── moon.pkg
├── docs/
│   └── taskq-moonbit-design.md
├── core/
│   ├── moon.pkg
│   ├── message.mbt
│   ├── task.mbt
│   ├── retry.mbt
│   ├── errors.mbt
│   └── stats.mbt
├── registry/
│   ├── moon.pkg
│   └── registry.mbt
├── runtime/
│   ├── moon.pkg
│   ├── dispatcher.mbt
│   ├── worker_pool.mbt
│   ├── hooks.mbt
│   └── shutdown.mbt
├── mem/
│   ├── moon.pkg
│   ├── queue.mbt
│   ├── scheduler.mbt
│   ├── dedupe.mbt
│   └── state.mbt
└── cmd/demo/
    ├── moon.pkg
    └── main.mbt
```

如果你想先快速推进，也可以先只用两个包：

- 根包：公共 API + runtime
- `mem/`：内存后端

## 3.2 公共 API 建议

Go 风格的 API 在 MoonBit 里建议改写为“显式任务注册 + 显式 codec”的形式。

示意接口如下：

```moonbit
pub trait Codec[A] {
  encode(A) -> Bytes raise
  decode(Bytes) -> A raise
}

pub struct Task[A] {
  id : Int
  name : String
}

pub struct TaskContext {
  queue_name : String
  job_id : String
  attempt : Int
  max_attempts : Int
  enqueued_at_ms : Int64
  started_at_ms : Int64
  deadline_ms : Int64?
  dedupe_key : String?
}

pub enum RetryPolicy {
  NoRetry
  Fixed(max_attempts : Int, delay_ms : Int)
  Exponential(
    max_attempts : Int,
    initial_delay_ms : Int,
    factor : Double,
    max_delay_ms : Int,
  )
}

pub struct TaskOptions[A] {
  name : String
  codec : Codec[A]
  retry : RetryPolicy
  handler : async (TaskContext, A) -> Unit raise
  fallback : (async (TaskContext, A, Error) -> Unit)?
}

pub struct EnqueueOptions {
  delay_ms : Int
  dedupe_key : String?
  schedule_at_ms : Int64?
  timeout_ms : Int?
  max_attempts_override : Int?
}

pub fn register_task[A](registry : Registry, options : TaskOptions[A]) -> Task[A]

pub async fn enqueue[A](
  queue : Queue,
  task : Task[A],
  payload : A,
  options? : EnqueueOptions,
) -> JobId raise
```

这个模型有几个优点：

- 类型安全，避免运行时反射错误。
- 任务序列化边界清晰，为未来远端后端铺路。
- 可以在同一公共 API 下兼容内存队列和持久化队列。

### 3.2.1 `Task` 的职责

这里的 `Task[A]` 不是一条消息，也不是 handler 本身，而是“任务类型的类型安全句柄”。

- `Task[A]` 代表一种可被投递的任务类别。
- `A` 是该任务的业务 payload 类型。
- `Task[A]` 本身不保存 payload，只保存任务身份。
- `enqueue(task, payload)` 时，`task` 用来找到注册表中的 codec、retry policy 和 handler。

建议把 `Task[A]` 设计得很薄，只保留：

- `id`
- `name`

这样做的原因是：

- 对外 API 简单。
- 运行时真正需要的复杂定义可以藏在注册表内部。
- `Task[A]` 可以安全地传给业务侧，而不会把内部可变状态暴露出去。

### 3.2.2 注册表内部需要的 erased 定义

由于运行时最终处理的是 `Bytes`，而不是泛型 `A`，所以注册表内部需要一个“擦除泛型后的任务定义”。

建议在内部实现一个非公开结构：

```moonbit
struct TaskSpec {
  id : Int
  name : String
  retry : RetryPolicy
  run : async (TaskContext, Bytes) -> Unit raise
  fallback : (async (TaskContext, Bytes, Error) -> Unit)?
}
```

`register_task[A]` 的工作，本质上是：

1. 为任务分配稳定的 `id`
2. 构造返回给业务侧的 `Task[A]`
3. 把 `TaskOptions[A]` 包装成内部 `TaskSpec`

包装逻辑大致如下：

```moonbit
fn erase_task[A](options : TaskOptions[A]) -> TaskSpec {
  {
    id: next_task_id(),
    name: options.name,
    retry: options.retry,
    run: async fn(ctx, payload) {
      let decoded = options.codec.decode(payload)
      options.handler(ctx, decoded)
    },
    fallback: options.fallback.map(fn(fb) {
      async fn(ctx, payload, err) {
        let decoded = options.codec.decode(payload)
        fb(ctx, decoded, err)
      }
    }),
  }
}
```

这个分层很关键：

- 对外暴露的是 `Task[A]`
- 注册表里存的是 `TaskSpec`
- 队列里流转的是 `JobEnvelope`

三者分别对应：

- 类型安全句柄
- 可执行定义
- 运行时实例

## 3.3 核心数据模型

建议定义统一的消息 envelope，而不是让内存后端直接存 handler 闭包。

```moonbit
pub struct JobEnvelope {
  id : String
  task_name : String
  payload : Bytes
  dedupe_key : String?
  created_at_ms : Int64
  visible_at_ms : Int64
  deadline_ms : Int64?
  attempt : Int
  max_attempts : Int
  last_error : String?
}
```

状态机建议如下：

```text
Accepted
  -> Ready
  -> Delayed
Accepted
  -> RejectedDuplicate
Accepted
  -> RejectedQueueClosed
Delayed
  -> Ready
Ready
  -> Running
Ready
  -> Cancelled
Running
  -> Succeeded
Running
  -> RetryScheduled
Running
  -> Dead
RetryScheduled
  -> Delayed
RetryScheduled
  -> Dead
Paused
  -> Ready
Paused
  -> Cancelled
```

这和 `taskq` 的 `Add / Release / Delete` 思想一致，只是 MoonBit 里可以把状态维护得更显式。

### 3.3.1 严格状态定义

建议把任务实例状态定义为以下完整集合：

```moonbit
pub enum JobState {
  Accepted
  RejectedDuplicate
  RejectedQueueClosed
  Delayed
  Ready
  Running
  RetryScheduled
  Paused
  Succeeded
  Dead
  Cancelled
}
```

各状态语义如下：

- `Accepted`
  任务已通过基本校验，进入队列系统，但尚未决定进入 `Ready` 还是 `Delayed`。
- `RejectedDuplicate`
  入队时命中去重规则，被拒绝接纳。终态。
- `RejectedQueueClosed`
  入队时队列已关闭或正在拒绝新任务。终态。
- `Delayed`
  任务已被系统接纳，但 `visible_at_ms > now`，不能执行。
- `Ready`
  任务可以被调度，但尚未实际分派给 worker。
- `Running`
  任务已分派给 worker，当前 attempt 正在执行。
- `RetryScheduled`
  当前 attempt 失败，但系统决定重试；正在计算并提交下一次执行计划。
- `Paused`
  任务本可运行，但队列因错误风暴或人工暂停而暂缓发车。
- `Succeeded`
  某个 attempt 成功结束。终态。
- `Dead`
  任务失败且不再重试，或 fallback 处理后确定终止。终态。
- `Cancelled`
  任务因 purge、shutdown 超时、人工取消或队列关闭策略而被丢弃。终态。

### 3.3.2 终态与非终态

终态：

- `RejectedDuplicate`
- `RejectedQueueClosed`
- `Succeeded`
- `Dead`
- `Cancelled`

非终态：

- `Accepted`
- `Delayed`
- `Ready`
- `Running`
- `RetryScheduled`
- `Paused`

一旦进入终态：

- 必须从 `jobs` 主表之外的运行队列结构中移除。
- 必须清理对应的 `inflight`、`ready_queue`、`delayed_heap` 索引。
- 必须按策略释放或保留 `dedupe_key`。

### 3.3.3 严格状态转移表

| 当前状态         | 触发事件                   | 条件                                                    | 下一状态              | 必做副作用                                                         |
| ---------------- | -------------------------- | ------------------------------------------------------- | --------------------- | ------------------------------------------------------------------ |
| `Accepted`       | `accept_now`               | `visible_at_ms <= now` 且队列未暂停                     | `Ready`               | 写入 `jobs`；压入 `ready_queue`；更新 `queued`                     |
| `Accepted`       | `accept_delayed`           | `visible_at_ms > now`                                   | `Delayed`             | 写入 `jobs`；压入 `delayed_heap`；更新 `delayed`                   |
| `Accepted`       | `reject_duplicate`         | 命中 `dedupe_key`                                       | `RejectedDuplicate`   | 返回重复错误；不进入主状态结构                                     |
| `Accepted`       | `reject_closed`            | 队列已关闭或拒绝新任务                                  | `RejectedQueueClosed` | 返回关闭错误；不进入主状态结构                                     |
| `Delayed`        | `timer_due`                | `visible_at_ms <= now` 且队列未暂停                     | `Ready`               | 从 `delayed_heap` 移除；压入 `ready_queue`                         |
| `Delayed`        | `timer_due_while_paused`   | `visible_at_ms <= now` 且队列已暂停                     | `Paused`              | 从 `delayed_heap` 移除；放入 `paused_ready` 或等价结构             |
| `Delayed`        | `cancel`                   | purge / shutdown 丢弃策略 / 人工取消                    | `Cancelled`           | 删除主表项；清理 dedupe；更新 `cancelled`                          |
| `Ready`          | `dispatch`                 | 有并发许可且 rate limit 允许                            | `Running`             | 从 `ready_queue` 出队；写入 `inflight`；`attempt_started_at = now` |
| `Ready`          | `pause_queue`              | 达到失败阈值或人工暂停                                  | `Paused`              | 从 `ready_queue` 转移到暂停区；标记队列暂停                        |
| `Ready`          | `cancel`                   | purge / shutdown 丢弃策略 / 人工取消                    | `Cancelled`           | 从 `ready_queue` 移除；清理 dedupe                                 |
| `Paused`         | `resume_queue`             | 管理员恢复或自动恢复                                    | `Ready`               | 重新压入 `ready_queue`；清除暂停标记                               |
| `Paused`         | `cancel`                   | purge / shutdown 丢弃策略 / 人工取消                    | `Cancelled`           | 从暂停区移除；清理 dedupe                                          |
| `Running`        | `ack_success`              | handler 成功返回                                        | `Succeeded`           | 移除 `inflight`；清理 dedupe；累加成功统计                         |
| `Running`        | `nack_retryable`           | handler 失败且 `attempt < max_attempts` 且错误可重试    | `RetryScheduled`      | 移除 `inflight`；记录 `last_error`；累加 `retried`                 |
| `Running`        | `nack_non_retryable`       | 错误不可重试                                            | `Dead`                | 移除 `inflight`；调用 fallback；清理 dedupe；累加失败统计          |
| `Running`        | `nack_attempt_exhausted`   | `attempt >= max_attempts`                               | `Dead`                | 移除 `inflight`；调用 fallback；清理 dedupe；累加失败统计          |
| `Running`        | `timeout_cancelled`        | shutdown 超时取消或显式取消策略                         | `Cancelled`           | 取消 async task；移除 `inflight`；清理 dedupe                      |
| `RetryScheduled` | `schedule_retry`           | 成功计算下一次 `visible_at_ms` 且 `visible_at_ms > now` | `Delayed`             | `attempt += 1`；写回 `visible_at_ms`；压入 `delayed_heap`          |
| `RetryScheduled` | `schedule_retry_immediate` | 下一次重试应立即执行                                    | `Ready`               | `attempt += 1`；压入 `ready_queue`                                 |
| `RetryScheduled` | `retry_aborted`            | fallback 决定终止或调度失败不可恢复                     | `Dead`                | 调用 fallback；清理 dedupe；累加失败统计                           |

### 3.3.4 状态转移约束

下面这些约束建议在实现中显式断言，否则后续很容易出现幽灵任务或重复消费：

1. `Running` 只能从 `Ready` 进入，不能从 `Delayed` 直接进入。
2. `Succeeded`、`Dead`、`Cancelled` 都必须是终态，不能再转出。
3. 任一时刻一个 job id 只能出现在一种运行索引里：
   - `delayed_heap`
   - `ready_queue`
   - `paused_ready`
   - `inflight`
4. `RetryScheduled` 不能长期驻留；它是一个“短暂的中间态”，只存在于 manager 处理 `Nack` 的事务窗口中。
5. `Paused` 表示“任务已可运行但被队列策略阻塞”，所以只能由 `Ready` 或“到期的 `Delayed`”进入。
6. `Accepted` 只存在于 enqueue 事务内，不应在稳定存储结构里长期可见。

### 3.3.5 全部合法转移图

```text
Accepted -> Ready
Accepted -> Delayed
Accepted -> RejectedDuplicate
Accepted -> RejectedQueueClosed

Delayed -> Ready
Delayed -> Paused
Delayed -> Cancelled

Ready -> Running
Ready -> Paused
Ready -> Cancelled

Paused -> Ready
Paused -> Cancelled

Running -> Succeeded
Running -> RetryScheduled
Running -> Dead
Running -> Cancelled

RetryScheduled -> Delayed
RetryScheduled -> Ready
RetryScheduled -> Dead
```

### 3.3.6 建议的实现断言

为了把这个表真正变成工程约束，建议提供一个内部转移函数：

```moonbit
fn transition(job : JobRecord, event : JobEvent, now_ms : Int64) -> JobState raise
```

要求：

- 所有状态变化必须经过 `transition`
- 非法转移直接抛内部错误
- 每次转移同时完成索引维护和 stats 更新

这样后续写测试时，可以直接对“状态 + 事件 -> 新状态 + 副作用”做白盒验证。

## 3.4 内存后端的数据结构

建议 `mem` 后端维护四类核心结构：

1. `ready_queue`
   FIFO 队列，存放可立即执行的 job id。
2. `delayed_heap`
   小顶堆，按 `visible_at_ms` 排序，用于 delay/retry 调度。
3. `inflight`
   `job_id -> lease/worker metadata`，表示正在执行。
4. `dedupe_index`
   `dedupe_key -> expire_at_ms`，用于去重。

还需要一个主表：

- `jobs : HashMap[JobId, JobEnvelope]`

这样做比“直接把消息对象到处传”更稳，原因是：

- 所有状态转移都有统一真相来源。
- 更容易做 stats、调试和未来持久化。
- ack/nack/retry 逻辑不会因为对象复制而失控。

## 3.5 运行时模型

推荐采用“单状态 owner + 多 worker 执行”的 actor 架构。

### 方案

- 一个 `queue manager` 独占管理所有队列状态。
- 多个 worker 只负责执行 handler，不直接改共享状态。
- worker 完成后通过 command 把 `Ack` / `Nack(err)` 发回 manager。

### 为什么这样比 Go 风格更适合 MoonBit

- 避免大量锁和竞争条件。
- 与 `TaskGroup` 的结构化并发天然匹配。
- pause / retry / shutdown / metrics 都能在 manager 中集中实现。

### 推荐 command 模型

```text
Enqueue(job)
Ack(job_id, timing)
Nack(job_id, err, timing)
PollStats(reply)
Shutdown(graceful, reply)
```

### 3.5.1 `command` 的设计意图

这里的 `command` 不是 shell 命令，也不是对业务侧直接暴露的公共 API。它是队列运行时内部发给 `queue manager` 的控制消息。

建议把 `queue manager` 视为整个队列状态的唯一 owner：

- 只有 manager 可以修改 `jobs`
- 只有 manager 可以修改 `ready_queue`
- 只有 manager 可以修改 `delayed_heap`
- 只有 manager 可以修改 `inflight`
- 只有 manager 可以最终决定一个 job 的状态转移

其他角色不直接改状态：

- 对外 `enqueue(...)` API 不直接往 `ready_queue` 塞 job
- worker 不直接把 job 从 `inflight` 删除
- retry 逻辑不直接改 `visible_at_ms`
- shutdown 逻辑不直接清空各种索引

它们只能把“意图”发给 manager，由 manager 串行执行。这就是 `command` 的职责。

换句话说：

- `command` 表示“某个角色希望队列发生一件事”
- manager 负责把这个请求解释成合法的状态转移和副作用

这套设计的根本目的，是把所有状态变化集中到一个地方，避免并发下的共享状态错乱。

### 3.5.2 为什么不能跳过 `command`

如果没有 `command` 这一层，系统很容易退化成多个角色同时修改队列状态：

- enqueue 路径自己改 `ready_queue`
- worker 成功后自己删 `inflight`
- worker 失败后自己改 `delayed_heap`
- shutdown 路径自己扫描并移除任务

这样会带来几类典型问题：

- 同一个 job 可能被重复移除或重复入队
- job 已经成功，但另一个并发路径又把它安排重试
- `ready_queue`、`delayed_heap`、`inflight` 与 `jobs` 主表之间失去一致性
- stats 更新被分散到多个地方，结果难以保证准确

`command` 的价值，就是把这些修改都收拢回 manager，使系统变成：

- 外部角色提交请求
- manager 统一执行状态转移
- 状态、副作用、统计在一个事务边界内落地

### 3.5.3 `command` 与状态机的关系

可以把 `command` 理解成“驱动状态机前进的事件输入”。

例如：

- `Enqueue(job)` 驱动 `Accepted -> Ready` 或 `Accepted -> Delayed`
- `Ack(job_id, timing)` 驱动 `Running -> Succeeded`
- `Nack(job_id, err, timing)` 驱动 `Running -> RetryScheduled -> Delayed/Ready`，或者 `Running -> Dead`
- `Shutdown(graceful, reply)` 驱动队列进入拒绝新任务和清理阶段

也就是说：

- 状态机定义“允许发生什么”
- `command` 表达“现在要发生什么”
- manager 负责检查“这次请求是否构成合法转移”

因此 `command` 和前面的严格状态转移表应当一一对应，而不是各自独立演化。

### 3.5.4 建议的完整 `command` 集合

在文档前面的简化模型基础上，建议在实现里使用更完整的内部 command 集合：

```moonbit
enum Command {
  Enqueue(JobEnvelope)
  Ack(job_id : String, timing : ProcessTiming)
  Nack(job_id : String, err : Error, timing : ProcessTiming)
  PollStats(reply : ReplyChan[StatsSnapshot])
  Pause(reason : PauseReason)
  Resume
  Shutdown(graceful : Bool, timeout_ms : Int, reply : ReplyChan[Unit])
  Cancel(job_id : String, reason : CancelReason)
}
```

其中：

- `Enqueue`
  表示“接纳一个新 job”
- `Ack`
  表示“某个 running attempt 已成功完成”
- `Nack`
  表示“某个 running attempt 已失败，请由 manager 决定是否重试”
- `PollStats`
  表示“请求当前状态快照”
- `Pause`
  表示“暂停从可运行队列继续派发”
- `Resume`
  表示“解除暂停，把暂停区里的任务恢复到 `Ready`”
- `Shutdown`
  表示“停止接收新任务，并按策略关闭运行时”
- `Cancel`
  表示“取消一个尚未完成的 job”

注意：

- `Ack/Nack` 是 worker 对 manager 的回报，不是 worker 自己做状态收尾
- `Pause/Resume/Shutdown/Cancel` 都应该被视为 manager 的控制面输入

### 3.5.5 一个最小的运行视角

从运行时的角度看，这套模型可以理解为：

1. 所有外部动作先转成 `command`
2. `command` 进入 manager 的 mailbox
3. manager 逐条取出并执行
4. 每次执行都对应：
   - 校验当前状态
   - 决定下一状态
   - 更新索引
   - 更新 stats
   - 必要时触发 fallback、timeout、retry 调度

于是整套系统的“并发”体现在 worker 执行 handler 上，而整套系统的“状态一致性”则由 manager 的串行 command 处理保证。

manager 每轮做三件事：

1. 处理外部命令。
2. 把到期的 delayed job 移到 ready。
3. 在并发许可足够时，把 ready job 派发给 worker。

## 3.6 重试与退避策略

这里不要手写太多复杂控制流。MoonBit 已经提供了 `retry`、`with_timeout`、`TaskGroup`、取消传播等能力，但队列级重试仍建议由 queue manager 主导，而不是完全交给 handler 自己调用 `@async.retry`。

原因是队列系统需要掌握：

- 当前 attempt 次数
- 下一次可见时间
- dead-letter/fallback 时机
- stats 统计
- pause 阈值

因此建议：

- handler 执行本身是一次 attempt。
- attempt 失败后由 queue manager 根据 `RetryPolicy` 计算下一次 `visible_at_ms`。
- 超过上限则转入 `Dead`，并调用 fallback。

退避公式建议如下：

- `Fixed`: `next = now + delay_ms`
- `Exponential`: `next = now + min(max_delay, initial * factor^(attempt - 1))`

另外建议支持一个可选错误分类器：

```moonbit
pub trait RetryClassifier {
  is_retryable(Error) -> Bool
  delay_override_ms(Error) -> Int?
}
```

这个点是从 `taskq` 的 `Delayer` 思路借鉴来的。Go 版支持错误自己决定 delay；MoonBit 版可以把这个能力显式化。

## 3.7 去重语义

`taskq` 的 dedup 核心是“带名字的消息在某个语义窗口内只处理一次”。

MoonBit 版建议拆成两种机制：

1. 显式 `dedupe_key`
   调用者自己给唯一 key。
2. 辅助函数 `once_in_period`
   根据 `task_name + business_key + period_bucket` 生成 dedupe key。

接口示意：

```moonbit
pub fn once_in_period(
  task_name : String,
  business_key : String,
  period_ms : Int64,
  now_ms : Int64,
) -> String
```

内存后端的去重表建议保存到“任务最终完成或过期”为止。第一版里最实用的语义是：

- 同一 `dedupe_key` 的任务只要仍在 ready/delayed/running 任一状态，就禁止再次入队。
- 成功完成后删除 dedupe key。
- 如果希望“周期内只允许运行一次”，则用 `expire_at_ms` 保留到周期结束。

## 3.8 限流与并发控制

`taskq` 有全局 worker limit 和全局 rate limit；在内存版第一阶段里建议保留 API，但只实现单进程局部能力。

建议拆成两层：

- `max_concurrency`
  用 `Semaphore` 控制同时执行的 handler 数量。
- `rate_limit`
  用内存 token bucket 控制单位时间允许启动多少个 attempt。

注意这里限流的位置应该是“开始执行 attempt 之前”，而不是 enqueue 时。

## 3.9 优雅关闭

生产可用的内存队列非常看重 shutdown 语义。

建议支持两种关闭方式：

1. `close_now`
   不再接收新任务，取消尚未开始的等待，尽快停止 worker。
2. `close_gracefully(timeout_ms)`
   不再接收新任务，但允许 in-flight 和 ready 队列在超时前尽量跑完。

推荐行为：

- shutdown 开始后，enqueue 直接返回错误。
- delayed 任务不再新建长期 timer；由 manager 统一决定是否丢弃或继续跑完。
- 超时后取消仍在执行的 async task。

## 3.10 hook 与观测

这部分建议第一版就做，因为后面补会很痛。

建议保留 `taskq` 的 hook 思路：

```moonbit
pub trait Hook {
  before_process(ProcessEvent) -> Unit raise
  after_process(ProcessEvent, ProcessResult) -> Unit raise
}

pub struct ProcessEvent {
  queue_name : String
  task_name : String
  job_id : String
  dedupe_key : String?
  attempt : Int
  max_attempts : Int
  enqueued_at_ms : Int64
  visible_at_ms : Int64
  started_at_ms : Int64
  deadline_ms : Int64?
}

pub enum AttemptOutcome {
  Succeeded
  FailedWillRetry
  FailedDead
  Cancelled
}

pub struct ProcessResult {
  outcome : AttemptOutcome
  error_message : String?
  duration_ms : Int64
  finished_at_ms : Int64
  next_visible_at_ms : Int64?
}
```

### 3.10.1 `Hook` 的职责

`Hook` 是任务处理过程的旁路观察器，用于承载：

- 日志
- metrics
- tracing
- 审计
- 非侵入式业务埋点

它不负责：

- 修改 job 状态
- 决定是否重试
- 修改 `ready_queue`、`delayed_heap`、`inflight`
- 替代 `fallback`

换句话说：

- `Hook` 负责“记录发生了什么”
- `command + manager + 状态机` 负责“决定系统怎么演进”
- `fallback` 负责“任务彻底失败后做什么补救”

### 3.10.2 `ProcessEvent` 的定义意图

`ProcessEvent` 表示“一次 attempt 开始执行前，hook 能看到的上下文”。

建议它只包含执行和观测必要的信息：

- `queue_name`
  该任务来自哪个队列。
- `task_name`
  该任务属于哪种任务定义。
- `job_id`
  这次任务实例的唯一标识。
- `dedupe_key`
  如果启用了去重，记录其业务去重键。
- `attempt`
  当前是第几次尝试。
- `max_attempts`
  总尝试上限。
- `enqueued_at_ms`
  首次入队时间。
- `visible_at_ms`
  当前 attempt 对应的可见时间。
- `started_at_ms`
  当前 attempt 实际开始执行的时间。
- `deadline_ms`
  若存在超时约束，则记录 deadline。

默认不建议把完整 payload 直接放进 `ProcessEvent`：

- payload 可能很大
- payload 可能包含敏感信息
- 观测层不应该默认泄露业务明文

如果后续确实有需要，可以单独增加可选的摘要字段，例如：

- `payload_preview : String?`
- `payload_size : Int`

但不建议第一版把业务 payload 作为 hook 载荷的一部分。

### 3.10.3 `ProcessResult` 的定义意图

`ProcessResult` 表示“一次 attempt 完成后的最终观测结果”。

建议至少包含：

- `outcome`
  这次 attempt 的最终结果类别。
- `error_message`
  失败时的错误摘要，成功时为空。
- `duration_ms`
  本次 attempt 处理耗时。
- `finished_at_ms`
  本次 attempt 结束时刻。
- `next_visible_at_ms`
  若会重试，则记录下一次计划执行时间。

其中 `AttemptOutcome` 建议显式区分：

- `Succeeded`
  本次 attempt 成功，任务进入终态成功。
- `FailedWillRetry`
  本次 attempt 失败，但 manager 已决定后续重试。
- `FailedDead`
  本次 attempt 失败，且任务已进入 `Dead`。
- `Cancelled`
  本次 attempt 因 purge、shutdown 超时、人工取消等原因终止。

这里不要只用 `Result[Unit, Error]` 表达结果。因为对队列系统来说：

- “失败但还会重试”
- “失败且彻底终止”

这是两个完全不同的运维事件，必须在 hook 层面可区分。

### 3.10.4 `Hook` 的调用时机

建议只提供两个稳定且易理解的 hook 时机：

1. `before_process`
   在 `Ready -> Running` 后、真正调用 handler 前触发。
2. `after_process`
   在 manager 已经根据本次结果做出最终决策后触发。

推荐原因：

- `before_process` 能拿到准确的 attempt 上下文。
- `after_process` 不只是知道“handler 报没报错”，而是知道：
  - 是否成功
  - 是否会重试
  - 是否已经进入 `Dead`
  - 是否被取消

因此 `after_process` 最好在 manager 完成 `Ack/Nack` 处理后再触发，或者至少在 manager 已经得出最终 `AttemptOutcome` 后触发。

### 3.10.5 `Hook` 的错误边界

为了保证 hook 只是旁路观测，建议明确以下规则：

- hook 抛错不能改变任务状态机结果
- hook 抛错不能把成功任务变失败
- hook 抛错不能阻止重试、fallback、ack/nack 逻辑

实现建议：

- 捕获所有 hook 异常
- 将 hook 自身错误记入内部日志或单独统计项
- 保持任务原有状态决策不变

也就是说，hook 的故障只影响观测质量，不影响队列语义正确性。

### 3.10.6 多个 `Hook` 的执行约定

建议支持注册多个 hook，并定义清楚执行规则：

- 多个 hook 按注册顺序执行
- `before_process` 按顺序调用
- `after_process` 也按相同顺序调用
- 任一 hook 失败都被捕获，不中断后续 hook

第一版不建议把 hook 系统做得太复杂，不需要先做：

- hook 优先级
- 条件过滤
- 单独异步 hook sink

等基本运行时稳定后再加。

### 3.10.7 `Hook` 与 `Stats`、`Fallback`、`Command` 的关系

- `Hook`
  观测接口，记录单次 attempt 的处理前后信息。
- `Stats`
  运行时统计快照，由 manager 维护聚合结果。
- `Fallback`
  任务最终失败后的补救逻辑。
- `Command`
  驱动 manager 状态机的内部控制消息。

它们的边界应该保持清晰：

- hook 不负责维护 stats
- hook 不负责发 command
- fallback 不替代 hook
- stats 不替代逐次 attempt 的观测明细

`Stats` 至少包含：

- `queued`
- `delayed`
- `inflight`
- `processed`
- `succeeded`
- `retried`
- `failed`
- `dead`
- `active_workers`
- `paused`

stats 不建议散落在 worker 中单独维护，最好由 manager 集中更新并提供快照。

## 4. 一个推荐的执行流程

## 4.1 入队

1. 调用方通过 `Task[A] + payload` 发起 enqueue。
2. codec 把 payload 编码成 `Bytes`。
3. 生成 `JobEnvelope`。
4. 检查 `dedupe_key`。
5. 若 `visible_at_ms <= now`，进入 `ready_queue`，否则进入 `delayed_heap`。
6. 更新 stats。

## 4.2 调度

1. manager 周期性检查 `delayed_heap` 堆顶。
2. 所有已到期任务移入 `ready_queue`。
3. 若并发许可足够且 rate limit 允许，则派发给 worker。

## 4.3 执行

1. worker 从 registry 找到 `task_name` 对应任务定义。
2. codec 解码 payload。
3. 可选包裹 `with_timeout`。
4. 调用 async handler。
5. 通过 command 向 manager 发送 `Ack` 或 `Nack`。

## 4.4 失败

1. `Nack` 到达 manager。
2. 根据 classifier 和 retry policy 判断是否重试。
3. 若可重试，更新 attempt 和 `visible_at_ms`，放回 delayed heap。
4. 若不可重试，调用 fallback，进入 dead 状态。
5. 更新连续失败计数；达到阈值则 `paused = true`。

## 4.5 恢复

pause 后可以提供两种恢复策略：

- 手动 `resume()`
- 自动冷却一段时间后恢复

第一版建议先做手动恢复，逻辑更简单、更可控。

## 5. 第一版不建议照搬 `taskq` 的部分

下面这些点建议先不要硬抄：

- fetcher / worker 自动伸缩
  Go 版有多 goroutine 拉取模型，MoonBit 内存版不需要先做这个复杂度。
- reservation timeout / reaper
  远端队列需要 lease 续约与过期回收；本地内存版第一版可以只维护 in-flight task handle。
- message batching
  这是为 SQS/IronMQ 等远端后端优化的，不是内存后端优先级。
- 压缩
  内存队列第一版意义不大。
- 反射 handler 适配
  在 MoonBit 中不值得复制。

## 6. 建议的实现顺序

## M1: 核心模型

- 定义 `Task[A]`、`Registry`、`JobEnvelope`、`RetryPolicy`、`Stats`
- 先不做 delayed/retry，只跑最小 happy path

验收标准：

- 能注册任务
- 能 enqueue
- 能并发消费
- 能优雅关闭

## M2: 内存队列状态机

- 加入 `ready_queue`、`jobs`、`inflight`
- manager/worker 分离
- 加上 stats 快照

验收标准：

- 不丢本进程内的 ready job
- close_gracefully 可等待 in-flight 完成

## M3: 延迟与重试

- 加入 `delayed_heap`
- 实现 `Fixed` / `Exponential` retry
- 加入 timeout 与 fallback

验收标准：

- delay 精度在合理范围内
- retry attempt 与 backoff 正确
- 超过上限进入 dead

## M4: 去重、暂停、限流

- `dedupe_key`
- `once_in_period`
- error storm pause
- local token bucket

验收标准：

- 重复任务按预期拒绝
- 连续失败触发 pause
- 恢复后可继续消费

## M5: 工程化

- hook
- benchmark
- admin/introspection API
- fake clock 测试基础设施

验收标准：

- 关键状态转移都有测试
- 具备基本性能基线

## 7. 测试策略

建议测试分三层：

### 7.1 纯状态机单测

不跑 async runtime，只测纯函数：

- backoff 计算
- dedupe key 生成
- 状态转移合法性
- pause 触发条件

### 7.2 async 集成测试

基于 `async test`：

- 并发上限是否生效
- delay/retry/fallback 是否按时执行
- graceful shutdown 是否等待 in-flight
- timeout 是否取消超时任务

### 7.3 压力与基准

- 高并发 enqueue
- 大量 delayed jobs
- 高频失败重试
- 去重表增长和清理

如果后续开始落代码，建议在每个阶段跑：

```bash
moon check
moon test
moon fmt
moon info
```

## 8. 我对这套 MoonBit 方案的最终建议

如果目标是“参考 `taskq`，但做一个适合 MoonBit 的生产级内存队列”，最合理的路线不是翻译 Go 代码，而是保留它的架构思想：

- `Task` 注册表
- `Message/JobEnvelope` 作为统一执行单元
- `Queue backend` 与 `Consumer runtime` 解耦
- 明确的 retry/delay/dedup/pause/fallback 语义

同时把实现方式改成更符合 MoonBit 的形式：

- `async` + `TaskGroup` + `Semaphore`
- 单 owner actor 管理队列状态
- 类型安全 codec 替代反射 handler
- 内存后端先实现单进程 at-least-once

一句话总结：

> 参考 `taskq` 时，应当继承它的“分层和语义”，不要继承它的“goroutine 形状”。

## 9. 建议的下一步

如果你准备继续实现，我建议下一步直接从下面三个文件开始建骨架：

- `core/task.mbt`
- `registry/registry.mbt`
- `mem/queue.mbt`

先把这些接口跑通：

1. `register_task`
2. `enqueue`
3. `start`
4. `close_gracefully`
5. `stats`

等这 5 个接口成立后，再加 delay/retry/dedupe，不要一开始把所有特性一起上。

## 参考资料

- `taskq` 仓库: <https://github.com/vmihailenco/taskq>
- `taskq` 包文档: <https://pkg.go.dev/github.com/vmihailenco/taskq/v3>
- `taskq` 入门文档: <https://taskq.uptrace.dev/guide/golang-task-queue.html>
- MoonBit 异步文档: <https://docs.moonbitlang.com/zh-cn/latest/language/async-experimental.html>
- MoonBit `async` API 文档: <https://mooncakes.io/assets/moonbitlang/async/async.mbt.html>
- MoonBit `task_group` API 文档: <https://beta.mooncakes.io/assets/moonbitlang/async/task_group.mbt.html>
