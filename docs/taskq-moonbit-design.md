# MoonBit 生产任务队列迁移设计

本文替换此前“单进程内存队列”方案。此前方案适合作为语义验证原型，但不适合作为生产迁移目标，原因很明确：

- 入队后不会自动持续消费，依赖显式 `process_next/process_all` 驱动。
- 仅有进程内状态，没有 durable storage，进程重启即丢任务。
- 不具备多进程/多实例 consumer 协调能力。
- 没有 reservation lease / visibility timeout / redelivery。
- 没有长期驻留的 consumer runtime、后台 fetch loop、worker supervision。

如果目标是“迁移生产可用的任务队列”，设计起点必须改成：

- durable queue first
- long-running consumer first
- multi-process correctness first
- observability and operations first

当前仓库中的 `mem` 实现应被视为：

- API 原型
- 状态机实验场
- 单测与 fake clock 载体

而不是最终生产后端。

## 1. 目标与非目标

## 1.1 目标

本项目的新目标是实现一个可在生产中运行的任务队列系统，至少满足：

1. 任务 durable 持久化。
2. consumer 常驻运行，任务入队后可自动持续消费。
3. 至少一次投递语义。
4. 多实例 consumer 可并发工作，且不会长期丢任务。
5. 任务支持延迟、重试、fallback、dead-letter。
6. 支持优雅关闭、暂停、恢复、限流、并发控制。
7. 提供可观测性：stats、hooks、日志、metrics、admin API。
8. API 与 runtime 分层，允许后续增加不同后端。

## 1.2 非目标

第一版不追求：

- exactly-once
- 跨地域多活一致性
- 分布式事务框架
- 与 Go `taskq` 的 API 逐字符兼容
- 先做很多后端

第一版只做一个生产后端即可，但这个后端必须 durable。优先顺序建议是：

1. PostgreSQL
2. Redis
3. 其他云队列后端

如果没有明确基础设施约束，我建议优先 PostgreSQL。原因：

- 事务语义清晰
- 去重与调度表达能力强
- 管理与排障成本低
- 对 MoonBit 首版实现更稳

## 2. 新的产品定义

系统应被定义为“任务系统”，而不是“一个带 `process_all()` 的队列对象”。

最低交付物应包含四层：

1. Producer API
   业务侧注册任务并入队。
2. Durable backend
   持久化消息、lease、schedule、retry、dedupe。
3. Consumer runtime
   常驻后台拉取、执行、ack/nack、续租、恢复。
4. Admin/observability
   stats、hooks、health、pause/resume、inspect、benchmark。

这四层缺一不可。

## 3. 核心运行语义

## 3.1 投递语义

第一版明确采用：

- at-least-once delivery

这意味着：

- handler 必须默认按幂等方式设计
- consumer 失败、超时、崩溃时，任务可以被重新投递
- 系统必须避免“静默丢任务”，而不是避免“一切重复”

## 3.2 任务生命周期

任务实例 `Job` 的推荐状态：

```text
Pending
Scheduled
Reserved
Running
RetryWaiting
Succeeded
Dead
Cancelled
```

语义定义：

- `Pending`
  可立即被拉取执行。
- `Scheduled`
  未来某个时刻才可见。
- `Reserved`
  已被某个 consumer lease 走，但 handler 可能尚未真正开始。
- `Running`
  handler 已开始执行。
- `RetryWaiting`
  本次失败，等待下一次可见时间。
- `Succeeded`
  终态。
- `Dead`
  重试耗尽或不可重试失败后的终态。
- `Cancelled`
  管理动作导致的终态。

`Reserved` 与 `Running` 必须保留。此前原型把“分派”和“执行”压得太近，不足以支撑生产下的 lease、续租、超时回收。

## 3.3 Reservation / visibility timeout

生产队列必须有 lease 语义：

- consumer 拉取任务时，不是立刻删除，而是先 `reserve`
- reservation 带 `lease_id` 和 `lease_deadline_ms`
- 在 lease 过期前，其他 consumer 不能再次拿到该任务
- 若 consumer 崩溃或未 ack，lease 到期后任务自动重新可见

这是生产正确性的核心，不是可选增强项。

## 3.4 Ack / Nack

- `Ack`
  任务成功完成，转为 `Succeeded`。
- `Nack(retryable)`
  任务失败，但允许重试，进入 `RetryWaiting`。
- `Nack(dead)`
  任务失败，且不再重试，进入 `Dead`。

ack/nack 必须由 backend 中央落地，不能只靠内存状态。

## 3.5 续租

如果 handler 可能跑很久，consumer runtime 必须支持 lease heartbeat：

- worker 周期性上报“仍在执行”
- backend 延长 `lease_deadline_ms`
- 若 heartbeat 停止，则 lease 超时并触发 redelivery

第一版可以限制：

- 默认任务超时较短
- 只在运行时间超过阈值时续租

但不能完全没有续租设计。

## 4. 新架构

## 4.1 模块划分

建议把系统拆成六个包：

1. `core`
   公共类型、错误、retry、hook、stats、job schema。
2. `registry`
   `Task[A]` 与 erased `TaskSpec` 注册。
3. `backend`
   durable backend trait。
4. `consumer`
   常驻 consumer runtime。
5. `backends/postgres`
   第一版生产后端。
6. `testing`
   fake clock、backend contract tests、stress helpers。

现有 `mem` 包调整定位：

- 不再作为生产目标
- 改为 `backends/memtest` 或保留 `mem`
- 只用于状态机测试、本地 demo、快速回归

## 4.2 推荐目录

```text
src/
├── moon.pkg
├── core/
│   ├── moon.pkg
│   ├── job.mbt
│   ├── retry.mbt
│   ├── hooks.mbt
│   ├── stats.mbt
│   └── errors.mbt
├── registry/
│   ├── moon.pkg
│   └── registry.mbt
├── backend/
│   ├── moon.pkg
│   ├── interface.mbt
│   └── lease.mbt
├── consumer/
│   ├── moon.pkg
│   ├── service.mbt
│   ├── worker_pool.mbt
│   ├── fetch_loop.mbt
│   ├── ack_loop.mbt
│   ├── heartbeat.mbt
│   └── shutdown.mbt
├── backends/postgres/
│   ├── moon.pkg
│   ├── schema.mbt
│   ├── enqueue.mbt
│   ├── reserve.mbt
│   ├── ack.mbt
│   ├── retry.mbt
│   └── admin.mbt
├── mem/
│   ├── moon.pkg
│   └── ...
└── testing/
    ├── moon.pkg
    └── ...
```

## 5. Producer API

对业务侧仍保留类型安全 API：

```moonbit
pub(all) struct Task[A] {
  id : Int
  name : String
  codec : Codec[A]
  retry : RetryPolicy
}

pub fn[A] register_task(
  registry : Registry,
  options : TaskOptions[A],
) -> Task[A] raise

pub async fn[A] enqueue(
  producer : Producer,
  task : Task[A],
  payload : A,
  options : EnqueueOptions,
) -> JobId raise
```

但对外对象要分开：

- `Producer`
- `ConsumerService`
- `AdminClient`

不能再让一个 `MemQueue` 同时承担“存储、调度、运行、管理”四种职责。

## 6. Durable backend 接口

推荐的 backend trait 不是简单的 `enqueue/dequeue`，而应围绕生产语义展开：

```moonbit
pub(open) trait QueueBackend {
  enqueue(JobRecord) -> JobId raise
  reserve_batch(
    queue_name : String,
    consumer_id : String,
    limit : Int,
    lease_ms : Int,
    now_ms : Int64,
  ) -> Array[Lease] raise
  ack(lease_id : String, finished_at_ms : Int64) -> Unit raise
  nack_retry(
    lease_id : String,
    error_message : String,
    next_visible_at_ms : Int64,
    finished_at_ms : Int64,
  ) -> Unit raise
  nack_dead(
    lease_id : String,
    error_message : String,
    finished_at_ms : Int64,
  ) -> Unit raise
  extend_lease(
    lease_id : String,
    new_deadline_ms : Int64,
  ) -> Unit raise
  reap_expired_leases(now_ms : Int64) -> Int raise
  pause(queue_name : String, reason : String) -> Unit raise
  resume(queue_name : String) -> Unit raise
  stats(queue_name : String) -> StatsSnapshot raise
}
```

关键点：

- `reserve_batch` 必须原子地拿任务并设置 lease
- `ack/nack` 必须基于 `lease_id`，不是只基于 `job_id`
- `extend_lease` 是生产必需，不是附属功能

## 7. Consumer runtime

## 7.1 正确的运行模型

生产运行时必须是 `start()` 驱动的常驻服务：

```moonbit
pub async fn ConsumerService::start(self : ConsumerService) -> Unit
pub async fn ConsumerService::stop(self : ConsumerService, timeout_ms : Int) -> Unit
```

而不是让调用方周期性调用 `process_all()`。

`start()` 后 runtime 内部至少有这些长期子循环：

1. fetch loop
   持续从 backend `reserve_batch(...)`
2. worker pool
   并发执行 handler
3. ack loop
   将执行结果统一提交到 backend
4. heartbeat loop
   为长任务续租
5. reaper/admin loop
   统计、暂停、恢复、过期回收

## 7.2 fetch loop

fetch loop 负责：

- 只要还有 worker capacity，就持续 reserve
- 没任务时 sleep + backoff
- paused/shutdown 时停止拉新

它必须是后台常驻的，而不是一次性 drain。

## 7.3 worker pool

worker 只负责：

- 解码 payload
- 调 handler
- 上报 `Ack` / `Nack`

worker 不直接修改 queue state，不直接改 durable row。

## 7.4 ack loop

所有任务结果统一进入 ack loop：

- 成功则 `ack`
- 可重试失败则 `nack_retry`
- 不可重试失败则 `nack_dead`
- 再触发 fallback / hook / metrics

这层的职责必须集中，不能散在 worker 里。

## 7.5 shutdown

支持两种：

1. `stop_now`
   停止 reserve，尽快取消未开始任务，in-flight 允许中断。
2. `stop_gracefully(timeout_ms)`
   停止 reserve，等待已有 in-flight 完成，超时后强制终止。

关闭语义必须可测试且可观测。

## 8. 数据模型

如果第一版采用 PostgreSQL，建议至少有一张主表：

```text
jobs
```

字段建议：

- `job_id`
- `queue_name`
- `task_name`
- `payload`
- `status`
- `attempt`
- `max_attempts`
- `dedupe_key`
- `created_at_ms`
- `visible_at_ms`
- `lease_id`
- `lease_owner`
- `lease_deadline_ms`
- `started_at_ms`
- `finished_at_ms`
- `last_error`
- `timeout_ms`
- `trace_id`

另可选：

- `job_history`
- `dead_jobs`
- `job_events`

第一版为了实现速度，可以先不拆历史表，但必须至少保留：

- 当前状态
- 最后错误
- attempt
- lease 信息

## 9. 重试、超时、fallback

## 9.1 重试

保留：

- `NoRetry`
- `Fixed`
- `Exponential`

但重试决策必须在 runtime + backend 一起完成：

- runtime 计算 `next_visible_at_ms`
- backend 原子写回状态

## 9.2 超时

每次 attempt 应支持：

- task default timeout
- enqueue override timeout

超时后默认按失败处理，而不是静默 cancel。

## 9.3 fallback

fallback 只在终态失败时触发：

- 不可重试错误
- 重试耗尽

fallback 自身失败只能记录，不能把 `Dead` 再改回别的状态。

## 10. 去重

生产版去重必须重新定义，不再只是进程内 map：

1. 入队去重
   相同 `dedupe_key` 的活跃任务拒绝再次入队。
2. 周期去重
   `once_in_period(...)` 生成业务 key。

推荐规则：

- 活跃状态：`Pending/Scheduled/Reserved/Running/RetryWaiting`
- 这些状态持有 dedupe
- `Succeeded/Dead/Cancelled` 后释放 dedupe

如果需要“成功后在窗口内仍不可重复”，应额外支持：

- `dedupe_until_ms`

## 11. Pause / circuit breaker

pause 不应只是一个布尔值，而应是运行策略：

- manual pause
- failure-storm pause
- dependency-down pause

恢复策略：

- manual resume
- cooldown auto resume

至少要暴露：

- `pause(queue_name, reason)`
- `resume(queue_name)`
- `paused_since`
- `pause_reason`

## 12. Hook / observability

保留现有 hook 思路，但要扩展到生产定位：

```moonbit
pub(open) trait Hook {
  before_process(Self, ProcessEvent) -> Unit raise
  after_process(Self, ProcessEvent, ProcessResult) -> Unit raise
}
```

规则不变：

- hook 不能改变状态机结果
- hook 错误必须被捕获
- 多个 hook 按注册顺序执行

此外必须补充：

- queue-level stats
- worker-level stats
- backend reserve latency
- ack latency
- lease extension count
- expired lease redelivery count

## 13. Admin API

生产版必须提供管理接口，而不是只有测试快照：

- `stats(queue_name)`
- `list_jobs(status, limit, cursor)`
- `get_job(job_id)`
- `retry_dead_job(job_id)`
- `cancel_job(job_id)`
- `pause_queue(queue_name)`
- `resume_queue(queue_name)`
- `drain_queue(queue_name)`
- `health()`

这层可以先做包内 API，后续再挂 HTTP/CLI。

## 14. 测试策略

测试需要升级，不再只围绕单进程内存状态。

## 14.1 纯函数单测

- retry/backoff
- dedupe key
- timeout 计算
- 状态转移合法性

## 14.2 backend contract tests

对每个 backend 跑同一套契约测试：

- enqueue 后可 reserve
- reserve 后在 lease 内不可被第二个 consumer 拿到
- lease 到期后可 redeliver
- ack 后不可再次出现
- nack_retry 后在指定时间重试
- nack_dead 后进入 dead
- dedupe 正确释放

## 14.3 consumer integration tests

- `start()` 后自动消费
- 多 worker 并发执行
- graceful shutdown 等待 in-flight
- forced shutdown 会停止 reserve
- heartbeat 能续租长任务
- consumer 崩溃后任务被重新投递

## 14.4 故障测试

- backend 短暂不可用
- hook 抛错
- fallback 抛错
- worker panic / async cancel
- process restart 恢复

## 14.5 benchmark

至少测：

- enqueue throughput
- reserve/ack throughput
- delayed job throughput
- retry-heavy workload
- large payload sensitivity

## 15. 里程碑重排

此前的 M1-M5 要废弃，重新排期。

## P0: 设计重置

- 确认 durable backend 选型
- 确认 delivery 语义
- 确认 producer/consumer/admin 三层 API

验收：

- 文档冻结
- 明确当前 `mem` 只作原型

## P1: core + registry + backend contract

- 固化 `Task[A]`、`TaskSpec`、`JobRecord`、`Lease`
- 设计 `QueueBackend` trait
- 写 backend contract tests

验收：

- contract tests 可在 fake backend 上跑通

## P2: 生产 backend MVP

- 实现 PostgreSQL backend
- enqueue / reserve / ack / nack / dedupe
- 基础 stats

验收：

- 进程重启后任务不丢
- 两个 consumer 实例可并发消费

## P3: 常驻 consumer runtime

- `start/stop`
- fetch loop
- worker pool
- ack loop
- graceful shutdown

验收：

- 入队后不需要手动 `process_all`
- 服务可持续消费

## P4: 重试、延迟、超时、fallback

- fixed/exponential retry
- timeout
- fallback
- DLQ / dead state

验收：

- retry 与 delay 正确
- timeout 后可重试或 dead

## P5: lease heartbeat 与 redelivery

- extend lease
- expired lease reaper
- worker crash recovery

验收：

- consumer 中途崩溃不丢任务
- 长任务不会被误重投

## P6: operations

- hooks
- admin API
- pause/resume
- benchmark
- metrics

验收：

- 可观测、可排障、可手工干预

## 16. 对当前代码库的影响

从这版设计开始，当前代码应按如下方式理解：

- `src/mem/*`
  保留为状态机与 API 原型，不再承诺生产可用。
- `process_next/process_all`
  保留给测试、demo、fake backend，不作为生产主接口。
- 新的生产主线应围绕：
  - `Producer`
  - `QueueBackend`
  - `ConsumerService::start()`
  - `AdminClient`

如果后续继续实现，优先顺序不再是“补内存特性”，而是：

1. 新建 `backend/` 抽象
2. 新建 `consumer/` 常驻 runtime
3. 选定并实现第一个 durable backend

## 17. 最终结论

此前设计的问题不在于“缺几个特性”，而在于目标被设成了“单进程内存队列”，这天然会把系统带向原型。

新的迁移目标必须明确：

- 生产可用的任务队列不是 `enqueue + process_all`
- 它必须是 `durable backend + resident consumer runtime + lease/ack/nack + operations`

一句话总结：

> 当前内存实现可以保留，但它不再是目标产品，只是下一版生产系统的验证底座。
