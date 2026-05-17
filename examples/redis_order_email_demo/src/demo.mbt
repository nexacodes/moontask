///|
pub let redis_host : String = "127.0.0.1"

///|
pub let redis_port : Int = 6379

///|
pub let redis_prefix : String = "moontask:demo:redis-bulk"

///|
pub let queue_name : String = "bulk-emails"

///|
pub let task_name : String = "email.bulk_send"

///|
pub struct BulkEmailPayload {
  sequence_no : Int
  user_email : String
  subject : String
  body : String
  simulated_work_ms : Int
} derive(ToJson, FromJson, Debug)

///|
pub fn new_demo_backend() -> @redis.RedisBackend {
  @redis.new_redis_backend(redis_host, redis_port, prefix=redis_prefix)
}

///|
pub fn bulk_email_codec() -> @core.Codec[BulkEmailPayload] {
  {
    encode: fn(payload : BulkEmailPayload) {
      @utf8.encode(payload.to_json().stringify())
    },
    decode: fn(payload : Bytes) raise {
      let json_text = @utf8.decode(payload)
      let json_value = @json.parse(json_text)
      @json.from_json(json_value)
    },
  }
}

///|
pub fn build_payload(sequence_no : Int) -> BulkEmailPayload {
  {
    sequence_no,
    user_email: "user-\{sequence_no}@example.com",
    subject: "bulk message #\{sequence_no}",
    body: "hello from redis bulk demo message #\{sequence_no}",
    simulated_work_ms: 5 + sequence_no % 10,
  }
}

///|
fn redis_failure(op : String, err : String) -> Error {
  @core.QueueError::BackendFailure("\{op}: \{err}")
}

///|
pub fn register_producer_task(
  registry : @registry.Registry,
) -> @core.Task[BulkEmailPayload] raise {
  @registry.register_task(registry, {
    name: task_name,
    codec: bulk_email_codec(),
    retry: @core.Fixed(max_attempts=3, delay_ms=100),
    handler: fn(_context : @core.TaskContext, _payload : BulkEmailPayload) {
      ()
    },
    fallback: None,
  })
}

///|
pub fn register_consumer_task(
  registry : @registry.Registry,
  consumer_id : String,
) -> @core.Task[BulkEmailPayload] raise {
  @registry.register_task(registry, {
    name: task_name,
    codec: bulk_email_codec(),
    retry: @core.Fixed(max_attempts=3, delay_ms=100),
    handler: async fn(context : @core.TaskContext, payload : BulkEmailPayload) {
      @async.sleep(payload.simulated_work_ms)
      if payload.sequence_no <= 3 || payload.sequence_no % 100 == 0 {
        println(
          "[\{consumer_id}] processed seq=\{payload.sequence_no} job=\{context.job_id} email=\{payload.user_email} work_ms=\{payload.simulated_work_ms}",
        )
      }
    },
    fallback: Some(fn(
      context : @core.TaskContext,
      payload : BulkEmailPayload,
      err : Error,
    ) {
      println(
        "[\{consumer_id}] dead seq=\{payload.sequence_no} job=\{context.job_id} err=\{err.to_string()}",
      )
    }),
  })
}

///|
pub async fn reset_demo_keys() -> Int {
  let client = @redis.connect(redis_host, redis_port)
  defer client.close()
  let keys = match client.keys("\{redis_prefix}:*") {
    Ok(keys) => keys
    Err(err) => raise redis_failure("KEYS", err)
  }
  if keys.length() == 0 {
    return 0
  }
  match client.send(["DEL", ..keys]).to_int() {
    Ok(count) => count
    Err(err) => raise redis_failure("DEL", err)
  }
}

///|
pub async fn wait_until_processed(
  backend : @redis.RedisBackend,
  expected_count : Int,
  timeout_ms : Int,
) -> @core.StatsSnapshot {
  let admin = @admin.AdminClient::new(backend)
  let deadline = @async.now() + Int64::from_int(timeout_ms)
  let mut last_report = ""
  let poll_limit = Int::max(1, timeout_ms / 500 + 1)
  let mut final_stats = admin.queue_snapshot(queue_name).stats
  for _ in 0..<poll_limit {
    let stats = admin.queue_snapshot(queue_name).stats
    final_stats = stats
    let report = "progress succeeded=\{stats.succeeded}/\{expected_count} pending=\{stats.pending} reserved=\{stats.reserved} running=\{stats.running} retry_waiting=\{stats.retry_waiting} dead=\{stats.dead}"
    if report != last_report {
      println(report)
      last_report = report
    }
    if stats.succeeded >= expected_count {
      return stats
    }
    if stats.dead > 0 {
      return stats
    }
    if @async.now() >= deadline {
      return stats
    }
    @async.sleep(500)
  }
  final_stats
}
