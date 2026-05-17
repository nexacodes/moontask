///|
struct Order {
  order_id : String
  user_email : String
  amount_cents : Int
  items : Array[String]
} derive(ToJson, Debug)

///|
struct OrderEmailPayload {
  order_id : String
  user_email : String
  amount_cents : Int
  items : Array[String]
} derive(ToJson, FromJson, Debug)

///|
fn order_email_codec() -> @moontask.Codec[OrderEmailPayload] {
  {
    encode: fn(payload : OrderEmailPayload) {
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
struct OrderService[B] {
  producer : @moontask.Producer[B]
  email_task : @moontask.Task[OrderEmailPayload]
  mut next_order_sequence : Int
}

///|
fn[B] new_order_service(
  producer : @moontask.Producer[B],
  email_task : @moontask.Task[OrderEmailPayload],
) -> OrderService[B] {
  { producer, email_task, next_order_sequence: 1 }
}

///|
fn build_email_payload(order : Order) -> OrderEmailPayload {
  {
    order_id: order.order_id,
    user_email: order.user_email,
    amount_cents: order.amount_cents,
    items: order.items.copy(),
  }
}

///|
pub async fn[B : @moontask.QueueBackend] OrderService::create_order(
  self : OrderService[B],
  user_email : String,
  items : Array[String],
  amount_cents : Int,
) -> Order {
  let order_id = "ord-\{self.next_order_sequence}"
  self.next_order_sequence = self.next_order_sequence + 1
  let order : Order = {
    order_id,
    user_email,
    amount_cents,
    items: items.copy(),
  }
  let job_id = self.producer.enqueue_with_options(
    self.email_task,
    build_email_payload(order),
    {
      delay_ms: 0,
      dedupe_key: Some("order-email:\{order_id}"),
      schedule_at_ms: None,
      timeout_ms: Some(10_000),
      max_attempts_override: None,
    },
  )
  println(
    "created order=\{order_id} email=\{user_email} amount=\{amount_cents} job=\{job_id}",
  )
  order
}

///|
async fn[B : @moontask.QueueBackend] queue_stats(
  backend : B,
  queue_name : String,
) -> @moontask.StatsSnapshot {
  backend.stats(queue_name)
}

///|
async fn[B : @moontask.QueueBackend] wait_until_succeeded(
  backend : B,
  queue_name : String,
  expected_count : Int,
) -> Unit {
  for _ in 0..<100 {
    let stats = backend.stats(queue_name)
    if stats.succeeded >= expected_count {
      return
    }
    @async.sleep(20)
  }
  raise @core.QueueError::StateInvariant(
    "order email demo timed out before all emails were sent",
  )
}

///|
async fn main {
  let backend = @moontask.new_mem_backend()
  let registry = @moontask.new_registry()
  let email_task = try! @moontask.register_task(registry, {
    name: "email.order_confirmation",
    codec: order_email_codec(),
    retry: @core.Fixed(max_attempts=3, delay_ms=500),
    handler: fn(context : @moontask.TaskContext, payload : OrderEmailPayload) {
      println(
        "send order email job=\{context.job_id} order=\{payload.order_id} to=\{payload.user_email}",
      )
      println(
        "subject: Order \{payload.order_id} confirmed, total=\{payload.amount_cents} cents",
      )
      println("items: \{payload.items.to_json().stringify()}")
    },
    fallback: Some(fn(
      context : @moontask.TaskContext,
      payload : OrderEmailPayload,
      err : Error,
    ) {
      println(
        "dead email job=\{context.job_id} order=\{payload.order_id} err=\{err.to_string()}",
      )
    }),
  })
  let producer = @moontask.Producer::new(backend, "emails")
  let order_service = new_order_service(producer, email_task)
  let consumer = @moontask.ConsumerService::new(backend, registry, {
    ..@moontask.default_consumer_options("emails", "order-email-consumer"),
    max_concurrency: 2,
    reserve_batch_size: 2,
    lease_ms: 5_000,
    poll_interval_ms: 20,
    pause_poll_interval_ms: 20,
  })

  @async.with_task_group(async fn(group) {
    let start_task = group.spawn(
      async fn() { try! consumer.start() },
      no_wait=true,
    )
    let first_order = try! order_service.create_order(
      "alice@example.com",
      ["book", "pen"],
      2599,
    )
    let second_order = try! order_service.create_order(
      "bob@example.com",
      ["lamp"],
      4999,
    )
    println("placed orders: \{first_order.to_json().stringify()}")
    println("placed orders: \{second_order.to_json().stringify()}")
    try! wait_until_succeeded(backend, "emails", 2)
    consumer.stop(1000)
    ignore(start_task.wait())
  })

  let stats = try! queue_stats(backend, "emails")
  println("final queue stats: \{stats.to_json().stringify()}")
}
