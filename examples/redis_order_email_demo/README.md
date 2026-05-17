# Redis Distributed Producer/Consumer Demo

这个示例演示：

- 使用 `nexacodes/moontask/backends/redis` 作为后端
- 启动多个 `consumer` 进程模拟分布式消费
- `producer` 一次性发送 1000 条消息
- 由 `producer` 轮询 Redis 队列统计，确认消息是否全部被消费

## 目录

- `src/consumer_app`: consumer 进程，可启动多个实例
- `src/producer_app`: producer 进程，一次性批量发送消息并等待消费完成
- `src/reset_app`: 清理本示例使用的 Redis key，便于重复运行

## 运行前准备

本机没有 `redis-server` 时，可以直接用 Docker 起一个：

```bash
docker run --rm --name moontask-redis -p 6379:6379 redis:7-alpine
```

## 推荐运行方式

先进入示例目录：

```bash
cd examples/redis_order_email_demo
```

先清理一次示例 key：

```bash
moon run src/reset_app
```

开两个或更多 consumer 终端：

```bash
moon run src/consumer_app -- -id consumer-1 -concurrency 8
moon run src/consumer_app -- -id consumer-2 -concurrency 8
```

然后启动 producer，一次性发 1000 条消息：

```bash
moon run src/producer_app -- -count 1000 -timeout-ms 120000
```

如果一切正常，`producer_app` 会持续输出进度，最终看到类似：

```text
all messages processed successfully
```

如果想增加 consumer 实例，继续在新终端执行：

```bash
moon run src/consumer_app -- -id consumer-3 -concurrency 8
```

## 默认配置

- Redis host: `127.0.0.1`
- Redis port: `6379`
- Redis prefix: `moontask:demo:redis-bulk`
- Queue name: `bulk-emails`

这些值定义在 `src/demo.mbt` 里。
