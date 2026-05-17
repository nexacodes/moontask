///|
priv struct StoredLease {
  lease_id : String
  queue_name : String
  consumer_id : String
  leased_at_ms : Int64
  lease_deadline_ms : Int64
  job_id : String
}

///|
pub struct RedisBackend {
  host : String
  port : Int
  prefix : String
  pool : RedisClientPool
}

///|
pub fn new_redis_backend(
  host : String,
  port : Int,
  prefix? : String = "moontask",
  pool_size? : Int = 32,
) -> RedisBackend {
  { host, port, prefix, pool: new_pool(host, port, max_size=pool_size) }
}

///|
fn visible_status(status : @core.JobStatus) -> Bool {
  status is @core.Pending ||
  status is @core.Scheduled ||
  status is @core.RetryWaiting
}

///|
fn initial_job_status(job : @core.JobRecord) -> @core.JobStatus {
  if job.visible_at_ms > job.created_at_ms {
    @core.Scheduled
  } else {
    @core.Pending
  }
}

///|
fn normalized_job(job : @core.JobRecord) -> @core.JobRecord {
  {
    ..job,
    status: initial_job_status(job),
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    started_at_ms: None,
    finished_at_ms: None,
    deadline_ms: None,
  }
}

///|
fn encode_status(status : @core.JobStatus) -> String {
  match status {
    @core.Pending => "Pending"
    @core.Scheduled => "Scheduled"
    @core.Reserved => "Reserved"
    @core.Running => "Running"
    @core.RetryWaiting => "RetryWaiting"
    @core.Succeeded => "Succeeded"
    @core.Dead => "Dead"
    @core.Cancelled => "Cancelled"
  }
}

///|
fn decode_status(status : String) -> @core.JobStatus raise {
  match status {
    "Pending" => @core.Pending
    "Scheduled" => @core.Scheduled
    "Reserved" => @core.Reserved
    "Running" => @core.Running
    "RetryWaiting" => @core.RetryWaiting
    "Succeeded" => @core.Succeeded
    "Dead" => @core.Dead
    "Cancelled" => @core.Cancelled
    _ => raise @core.QueueError::BackendFailure("unknown job status \{status}")
  }
}

///|
fn encode_optional_string(value : String?) -> String {
  match value {
    Some(value) => value
    None => ""
  }
}

///|
fn decode_optional_string(value : String) -> String? {
  if value == "" {
    None
  } else {
    Some(value)
  }
}

///|
fn encode_optional_int(value : Int?) -> String {
  match value {
    Some(value) => value.to_string()
    None => ""
  }
}

///|
fn encode_optional_int64(value : Int64?) -> String {
  match value {
    Some(value) => value.to_string()
    None => ""
  }
}

///|
fn decode_int(value : String, name : String) -> Int raise {
  @string.parse_int(value) catch {
    _ =>
      raise @core.QueueError::BackendFailure(
        "invalid int for \{name}: \{value}",
      )
  }
}

///|
fn decode_int64(value : String, name : String) -> Int64 raise {
  @string.parse_int64(value) catch {
    _ =>
      raise @core.QueueError::BackendFailure(
        "invalid int64 for \{name}: \{value}",
      )
  }
}

///|
fn decode_optional_int(value : String, name : String) -> Int? raise {
  match decode_optional_string(value) {
    Some(value) => Some(decode_int(value, name))
    None => None
  }
}

///|
fn decode_optional_int64(value : String, name : String) -> Int64? raise {
  match decode_optional_string(value) {
    Some(value) => Some(decode_int64(value, name))
    None => None
  }
}

///|
fn job_key(backend : RedisBackend, job_id : String) -> String {
  "\{backend.prefix}:job:\{job_id}"
}

///|
fn job_key_prefix(backend : RedisBackend) -> String {
  "\{backend.prefix}:job:"
}

///|
fn queue_jobs_key(backend : RedisBackend, queue_name : String) -> String {
  "\{backend.prefix}:queue:\{queue_name}:jobs"
}

///|
fn queue_visible_key(backend : RedisBackend, queue_name : String) -> String {
  "\{backend.prefix}:queue:\{queue_name}:visible"
}

///|
fn pause_key(backend : RedisBackend, queue_name : String) -> String {
  "\{backend.prefix}:queue:\{queue_name}:paused"
}

///|
fn dedupe_key(backend : RedisBackend, dedupe_key : String) -> String {
  "\{backend.prefix}:dedupe:\{dedupe_key}"
}

///|
fn lease_key(backend : RedisBackend, lease_id : String) -> String {
  "\{backend.prefix}:lease:\{lease_id}"
}

///|
fn lease_key_prefix(backend : RedisBackend) -> String {
  "\{backend.prefix}:lease:"
}

///|
fn leases_key(backend : RedisBackend) -> String {
  "\{backend.prefix}:leases"
}

///|
fn lease_seq_key(backend : RedisBackend) -> String {
  "\{backend.prefix}:lease_seq"
}

///|
fn job_fields(job : @core.JobRecord) -> Array[String] {
  [
    "job_id",
    job.job_id,
    "queue_name",
    job.queue_name,
    "task_name",
    job.task_name,
    "payload",
    @base64.encode(job.payload),
    "status",
    encode_status(job.status),
    "attempt",
    job.attempt.to_string(),
    "max_attempts",
    job.max_attempts.to_string(),
    "dedupe_key",
    encode_optional_string(job.dedupe_key),
    "created_at_ms",
    job.created_at_ms.to_string(),
    "visible_at_ms",
    job.visible_at_ms.to_string(),
    "lease_id",
    encode_optional_string(job.lease_id),
    "lease_owner",
    encode_optional_string(job.lease_owner),
    "lease_deadline_ms",
    encode_optional_int64(job.lease_deadline_ms),
    "started_at_ms",
    encode_optional_int64(job.started_at_ms),
    "finished_at_ms",
    encode_optional_int64(job.finished_at_ms),
    "timeout_ms",
    encode_optional_int(job.timeout_ms),
    "deadline_ms",
    encode_optional_int64(job.deadline_ms),
    "last_error",
    encode_optional_string(job.last_error),
    "trace_id",
    encode_optional_string(job.trace_id),
  ]
}

///|
fn get_required_field(
  fields : Map[String, String],
  name : String,
) -> String raise {
  match fields.get(name) {
    Some(value) => value
    None => raise @core.QueueError::BackendFailure("missing field \{name}")
  }
}

///|
fn job_from_fields(fields : Map[String, String]) -> @core.JobRecord raise {
  {
    job_id: get_required_field(fields, "job_id"),
    queue_name: get_required_field(fields, "queue_name"),
    task_name: get_required_field(fields, "task_name"),
    payload: @base64.decode(get_required_field(fields, "payload")) catch {
      _ => raise @core.QueueError::BackendFailure("invalid job payload")
    },
    status: decode_status(get_required_field(fields, "status")),
    attempt: decode_int(get_required_field(fields, "attempt"), "attempt"),
    max_attempts: decode_int(
      get_required_field(fields, "max_attempts"),
      "max_attempts",
    ),
    dedupe_key: decode_optional_string(get_required_field(fields, "dedupe_key")),
    created_at_ms: decode_int64(
      get_required_field(fields, "created_at_ms"),
      "created_at_ms",
    ),
    visible_at_ms: decode_int64(
      get_required_field(fields, "visible_at_ms"),
      "visible_at_ms",
    ),
    lease_id: decode_optional_string(get_required_field(fields, "lease_id")),
    lease_owner: decode_optional_string(
      get_required_field(fields, "lease_owner"),
    ),
    lease_deadline_ms: decode_optional_int64(
      get_required_field(fields, "lease_deadline_ms"),
      "lease_deadline_ms",
    ),
    started_at_ms: decode_optional_int64(
      get_required_field(fields, "started_at_ms"),
      "started_at_ms",
    ),
    finished_at_ms: decode_optional_int64(
      get_required_field(fields, "finished_at_ms"),
      "finished_at_ms",
    ),
    timeout_ms: decode_optional_int(
      get_required_field(fields, "timeout_ms"),
      "timeout_ms",
    ),
    deadline_ms: decode_optional_int64(
      get_required_field(fields, "deadline_ms"),
      "deadline_ms",
    ),
    last_error: decode_optional_string(get_required_field(fields, "last_error")),
    trace_id: decode_optional_string(get_required_field(fields, "trace_id")),
  }
}

///|
fn stored_lease_from_fields(fields : Map[String, String]) -> StoredLease raise {
  {
    lease_id: get_required_field(fields, "lease_id"),
    queue_name: get_required_field(fields, "queue_name"),
    consumer_id: get_required_field(fields, "consumer_id"),
    leased_at_ms: decode_int64(
      get_required_field(fields, "leased_at_ms"),
      "leased_at_ms",
    ),
    lease_deadline_ms: decode_int64(
      get_required_field(fields, "lease_deadline_ms"),
      "lease_deadline_ms",
    ),
    job_id: get_required_field(fields, "job_id"),
  }
}

///|
fn raise_backend_failure(op : String, err : String) -> Error {
  @core.QueueError::BackendFailure("\{op}: \{err}")
}

///|
async fn open_client(backend : RedisBackend) -> RedisClient {
  backend.pool.acquire()
}

///|
async fn hset_all(
  client : RedisClient,
  key : String,
  fields : Array[String],
) -> Unit {
  match client.send(["HSET", key, ..fields]).to_int() {
    Ok(_) => ()
    Err(err) => raise raise_backend_failure("HSET \{key}", err)
  }
}

///|
async fn get_array(
  client : RedisClient,
  args : Array[String],
  op : String,
) -> Array[String] {
  match client.send(args).to_array() {
    Ok(value) => value
    Err(err) => raise raise_backend_failure(op, err)
  }
}

///|
async fn get_hash(client : RedisClient, key : String) -> Map[String, String] {
  match client.hgetall(key) {
    Ok(value) => value
    Err(err) => raise raise_backend_failure("HGETALL \{key}", err)
  }
}

///|
async fn get_int(
  client : RedisClient,
  args : Array[String],
  op : String,
) -> Int {
  match client.send(args).to_int() {
    Ok(value) => value
    Err(err) => raise raise_backend_failure(op, err)
  }
}

///|
async fn load_job(
  backend : RedisBackend,
  client : RedisClient,
  job_id : String,
) -> @core.JobRecord? {
  let fields = get_hash(client, job_key(backend, job_id))
  if fields.is_empty() {
    None
  } else {
    Some(job_from_fields(fields))
  }
}

///|
async fn load_job_required(
  backend : RedisBackend,
  client : RedisClient,
  job_id : String,
) -> @core.JobRecord {
  match load_job(backend, client, job_id) {
    Some(job) => job
    None => raise @core.QueueError::StateInvariant("missing job \{job_id}")
  }
}

///|
async fn load_stored_lease(
  backend : RedisBackend,
  client : RedisClient,
  lease_id : String,
) -> StoredLease? {
  let fields = get_hash(client, lease_key(backend, lease_id))
  if fields.is_empty() {
    None
  } else {
    Some(stored_lease_from_fields(fields))
  }
}

///|
async fn load_stored_lease_required(
  backend : RedisBackend,
  client : RedisClient,
  lease_id : String,
) -> StoredLease {
  match load_stored_lease(backend, client, lease_id) {
    Some(lease) => lease
    None => raise @core.QueueError::LeaseNotFound(lease_id)
  }
}

///|
fn build_stats_snapshot(
  jobs : Array[@core.JobRecord],
  paused : Bool,
) -> @core.StatsSnapshot {
  let mut pending = 0
  let mut scheduled = 0
  let mut reserved = 0
  let mut running = 0
  let mut retry_waiting = 0
  let mut succeeded = 0
  let mut retried = 0
  let mut failed = 0
  let mut dead = 0
  let mut cancelled = 0
  for job in jobs {
    match job.status {
      @core.Pending => pending = pending + 1
      @core.Scheduled => scheduled = scheduled + 1
      @core.Reserved => reserved = reserved + 1
      @core.Running => running = running + 1
      @core.RetryWaiting => {
        retry_waiting = retry_waiting + 1
        retried = retried + 1
      }
      @core.Succeeded => succeeded = succeeded + 1
      @core.Dead => {
        dead = dead + 1
        failed = failed + 1
      }
      @core.Cancelled => cancelled = cancelled + 1
    }
  }
  {
    pending,
    scheduled,
    reserved,
    running,
    retry_waiting,
    succeeded,
    retried,
    failed,
    dead,
    cancelled,
    paused,
  }
}

///|
async fn load_queue_jobs(
  backend : RedisBackend,
  client : RedisClient,
  queue_name : String,
) -> Array[@core.JobRecord] {
  let job_ids = match client.smembers(queue_jobs_key(backend, queue_name)) {
    Ok(job_ids) => job_ids
    Err(err) => raise raise_backend_failure("SMEMBERS queue jobs", err)
  }
  let jobs = []
  for job_id in job_ids {
    match load_job(backend, client, job_id) {
      Some(job) => jobs.push(job)
      None => ()
    }
  }
  jobs
}

///|
async fn claim_dedupe(
  backend : RedisBackend,
  client : RedisClient,
  dedupe_key_value : String,
  job_id : String,
) -> Bool {
  match
    client
    .send(["SET", dedupe_key(backend, dedupe_key_value), job_id, "NX"])
    .to_string() {
    Ok(_) => true
    Err("Type mismatch: expected string") => false
    Err(err) => raise raise_backend_failure("SET NX dedupe", err)
  }
}

///|
async fn release_dedupe(
  backend : RedisBackend,
  client : RedisClient,
  dedupe_key_value : String?,
) -> Unit {
  match dedupe_key_value {
    Some(dedupe_key_value) =>
      ignore(
        get_int(
          client,
          ["DEL", dedupe_key(backend, dedupe_key_value)],
          "DEL dedupe",
        ),
      )
    None => ()
  }
}

///|
async fn set_job(
  backend : RedisBackend,
  client : RedisClient,
  job : @core.JobRecord,
) -> Unit {
  hset_all(client, job_key(backend, job.job_id), job_fields(job))
}

///|
const RESERVE_BATCH_SCRIPT = "if redis.call('EXISTS', KEYS[1]) == 1 then return {} end local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2]) local result = {} for _, job_id in ipairs(ids) do redis.call('ZREM', KEYS[2], job_id) local lease_seq = redis.call('INCR', KEYS[3]) local lease_id = 'lease-' .. lease_seq local deadline = ARGV[1] + ARGV[3] local job_key = KEYS[4] .. job_id redis.call('HSET', job_key, 'status', ARGV[4], 'lease_id', lease_id, 'lease_owner', ARGV[5], 'lease_deadline_ms', tostring(deadline), 'started_at_ms', '', 'finished_at_ms', '', 'deadline_ms', '') local lease_key = KEYS[5] .. lease_id redis.call('HSET', lease_key, 'lease_id', lease_id, 'queue_name', ARGV[6], 'consumer_id', ARGV[5], 'leased_at_ms', tostring(ARGV[1]), 'lease_deadline_ms', tostring(deadline), 'job_id', job_id) redis.call('ZADD', KEYS[6], deadline, lease_id) table.insert(result, lease_id) table.insert(result, job_id) end return result"

///|
const REAP_EXPIRED_SCRIPT = "local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1]) for _, lease_id in ipairs(ids) do local lease_key = KEYS[2] .. lease_id local job_id = redis.call('HGET', lease_key, 'job_id') local queue_name = redis.call('HGET', lease_key, 'queue_name') if job_id and queue_name then local job_key = KEYS[3] .. job_id redis.call('HSET', job_key, 'status', ARGV[2], 'visible_at_ms', tostring(ARGV[1]), 'lease_id', '', 'lease_owner', '', 'lease_deadline_ms', '', 'deadline_ms', '') redis.call('ZADD', KEYS[4] .. queue_name .. ':visible', ARGV[1], job_id) end redis.call('ZREM', KEYS[1], lease_id) redis.call('DEL', lease_key) end return #ids"

///|
pub async fn RedisBackend::reset_namespace(self : RedisBackend) -> Unit {
  let client = open_client(self)
  defer client.close()
  let keys = match client.keys("\{self.prefix}:*") {
    Ok(keys) => keys
    Err(err) => raise raise_backend_failure("KEYS namespace", err)
  }
  if !keys.is_empty() {
    ignore(get_int(client, ["DEL", ..keys], "DEL namespace"))
  }
}

///|
pub impl @backend.QueueBackend for RedisBackend with enqueue(self, job) raise {
  let client = open_client(self)
  defer client.close()
  let job = normalized_job(job)
  match job.dedupe_key {
    Some(dedupe_key_value) => {
      let claimed = claim_dedupe(self, client, dedupe_key_value, job.job_id)
      guard claimed else {
        raise @core.QueueError::DuplicateJob(dedupe_key_value)
      }
    }
    None => ()
  }
  set_job(self, client, job)
  match client.sadd(queue_jobs_key(self, job.queue_name), [job.job_id]) {
    Ok(_) => ()
    Err(err) => raise raise_backend_failure("SADD queue jobs", err)
  }
  if visible_status(job.status) {
    ignore(
      get_int(
        client,
        [
          "ZADD",
          queue_visible_key(self, job.queue_name),
          job.visible_at_ms.to_string(),
          job.job_id,
        ],
        "ZADD visible",
      ),
    )
  }
  job.job_id
}

///|
pub impl @backend.QueueBackend for RedisBackend with reserve_batch(
  self,
  queue_name,
  consumer_id,
  limit,
  lease_ms,
  now_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let result = get_array(
    client,
    [
      "EVAL",
      RESERVE_BATCH_SCRIPT,
      "6",
      pause_key(self, queue_name),
      queue_visible_key(self, queue_name),
      lease_seq_key(self),
      job_key_prefix(self),
      lease_key_prefix(self),
      leases_key(self),
      now_ms.to_string(),
      Int::max(0, limit).to_string(),
      Int::max(0, lease_ms).to_string(),
      encode_status(@core.Reserved),
      consumer_id,
      queue_name,
    ],
    "EVAL reserve_batch",
  )
  let leases = []
  let lease_deadline_ms = now_ms + Int64::from_int(Int::max(0, lease_ms))
  let mut i = 0
  while i + 1 < result.length() {
    let lease_id = result[i]
    let job_id = result[i + 1]
    let job = load_job_required(self, client, job_id)
    let lease : @core.Lease = {
      lease_id,
      queue_name,
      consumer_id,
      leased_at_ms: now_ms,
      lease_deadline_ms,
      job,
    }
    leases.push(lease)
    i = i + 2
  }
  leases
}

///|
pub impl @backend.QueueBackend for RedisBackend with mark_running(
  self,
  lease_id,
  started_at_ms,
  deadline_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let lease = load_stored_lease_required(self, client, lease_id)
  ignore(
    get_int(
      client,
      [
        "HSET",
        job_key(self, lease.job_id),
        "status",
        encode_status(@core.Running),
        "started_at_ms",
        started_at_ms.to_string(),
        "deadline_ms",
        encode_optional_int64(deadline_ms),
      ],
      "HSET mark_running",
    ),
  )
  let job = load_job_required(self, client, lease.job_id)
  {
    lease_id: lease.lease_id,
    queue_name: lease.queue_name,
    consumer_id: lease.consumer_id,
    leased_at_ms: lease.leased_at_ms,
    lease_deadline_ms: lease.lease_deadline_ms,
    job,
  }
}

///|
pub impl @backend.QueueBackend for RedisBackend with ack(
  self,
  lease_id,
  finished_at_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let lease = load_stored_lease_required(self, client, lease_id)
  let job = load_job_required(self, client, lease.job_id)
  let updated_job : @core.JobRecord = {
    ..job,
    status: @core.Succeeded,
    finished_at_ms: Some(finished_at_ms),
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    deadline_ms: None,
  }
  set_job(self, client, updated_job)
  ignore(get_int(client, ["ZREM", leases_key(self), lease_id], "ZREM lease"))
  ignore(get_int(client, ["DEL", lease_key(self, lease_id)], "DEL lease"))
  release_dedupe(self, client, job.dedupe_key)
}

///|
pub impl @backend.QueueBackend for RedisBackend with nack_retry(
  self,
  lease_id,
  error_message,
  next_visible_at_ms,
  finished_at_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let lease = load_stored_lease_required(self, client, lease_id)
  let job = load_job_required(self, client, lease.job_id)
  let status = if next_visible_at_ms <= finished_at_ms {
    @core.Pending
  } else {
    @core.RetryWaiting
  }
  let updated_job : @core.JobRecord = {
    ..job,
    status,
    attempt: job.attempt + 1,
    visible_at_ms: next_visible_at_ms,
    finished_at_ms: Some(finished_at_ms),
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    deadline_ms: None,
    last_error: Some(error_message),
  }
  set_job(self, client, updated_job)
  ignore(
    get_int(
      client,
      [
        "ZADD",
        queue_visible_key(self, updated_job.queue_name),
        next_visible_at_ms.to_string(),
        updated_job.job_id,
      ],
      "ZADD retry visible",
    ),
  )
  ignore(get_int(client, ["ZREM", leases_key(self), lease_id], "ZREM lease"))
  ignore(get_int(client, ["DEL", lease_key(self, lease_id)], "DEL lease"))
}

///|
pub impl @backend.QueueBackend for RedisBackend with nack_dead(
  self,
  lease_id,
  error_message,
  finished_at_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let lease = load_stored_lease_required(self, client, lease_id)
  let job = load_job_required(self, client, lease.job_id)
  let updated_job : @core.JobRecord = {
    ..job,
    status: @core.Dead,
    finished_at_ms: Some(finished_at_ms),
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    deadline_ms: None,
    last_error: Some(error_message),
  }
  set_job(self, client, updated_job)
  ignore(get_int(client, ["ZREM", leases_key(self), lease_id], "ZREM lease"))
  ignore(get_int(client, ["DEL", lease_key(self, lease_id)], "DEL lease"))
  release_dedupe(self, client, job.dedupe_key)
}

///|
pub impl @backend.QueueBackend for RedisBackend with extend_lease(
  self,
  lease_id,
  new_deadline_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let lease = match load_stored_lease(self, client, lease_id) {
    Some(lease) => lease
    None => return
  }
  ignore(
    get_int(
      client,
      [
        "HSET",
        lease_key(self, lease_id),
        "lease_deadline_ms",
        new_deadline_ms.to_string(),
      ],
      "HSET lease deadline",
    ),
  )
  ignore(
    get_int(
      client,
      [
        "HSET",
        job_key(self, lease.job_id),
        "lease_deadline_ms",
        new_deadline_ms.to_string(),
      ],
      "HSET job lease deadline",
    ),
  )
  ignore(
    get_int(
      client,
      ["ZADD", leases_key(self), new_deadline_ms.to_string(), lease_id],
      "ZADD lease deadline",
    ),
  )
}

///|
pub impl @backend.QueueBackend for RedisBackend with reap_expired_leases(
  self,
  now_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  get_int(
    client,
    [
      "EVAL",
      REAP_EXPIRED_SCRIPT,
      "4",
      leases_key(self),
      lease_key_prefix(self),
      job_key_prefix(self),
      "\{self.prefix}:queue:",
      now_ms.to_string(),
      encode_status(@core.Pending),
    ],
    "EVAL reap_expired_leases",
  )
}

///|
pub impl @backend.QueueBackend for RedisBackend with pause(
  self,
  queue_name,
  reason,
) raise {
  let client = open_client(self)
  defer client.close()
  match client.set(pause_key(self, queue_name), reason) {
    Ok(_) => ()
    Err(err) => raise raise_backend_failure("SET pause", err)
  }
}

///|
pub impl @backend.QueueBackend for RedisBackend with resume_queue(
  self,
  queue_name,
) raise {
  let client = open_client(self)
  defer client.close()
  ignore(get_int(client, ["DEL", pause_key(self, queue_name)], "DEL pause"))
}

///|
pub impl @backend.QueueBackend for RedisBackend with stats(self, queue_name) raise {
  let client = open_client(self)
  defer client.close()
  let jobs = load_queue_jobs(self, client, queue_name)
  let paused = get_int(
      client,
      ["EXISTS", pause_key(self, queue_name)],
      "EXISTS pause",
    ) >
    0
  build_stats_snapshot(jobs, paused)
}

///|
pub impl @backend.QueueBackend for RedisBackend with list_jobs(
  self,
  queue_name,
  status,
  limit,
  cursor,
) raise {
  let client = open_client(self)
  defer client.close()
  let jobs = load_queue_jobs(self, client, queue_name)
  let filtered = []
  for job in jobs {
    let matches_status = match status {
      Some(status) => job.status == status
      None => true
    }
    let after_cursor = match cursor {
      Some(cursor) => job.job_id > cursor
      None => true
    }
    if matches_status && after_cursor {
      filtered.push(job)
    }
  }
  filtered.sort_by(fn(lhs, rhs) { lhs.job_id.compare(rhs.job_id) })
  let page_size = Int::max(0, limit)
  let page = []
  for i in 0..<Int::min(page_size, filtered.length()) {
    page.push(filtered[i])
  }
  let next_cursor = if filtered.length() > page.length() && !page.is_empty() {
    Some(page[page.length() - 1].job_id)
  } else {
    None
  }
  { jobs: page, next_cursor }
}

///|
pub impl @backend.QueueBackend for RedisBackend with get_job(self, job_id) raise {
  let client = open_client(self)
  defer client.close()
  load_job(self, client, job_id)
}

///|
pub impl @backend.QueueBackend for RedisBackend with retry_dead_job(
  self,
  job_id,
  now_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let job = match load_job(self, client, job_id) {
    Some(job) => job
    None => return false
  }
  guard job.status is @core.Dead else { return false }
  match job.dedupe_key {
    Some(dedupe_key_value) => {
      let claimed = claim_dedupe(self, client, dedupe_key_value, job_id)
      guard claimed else {
        raise @core.QueueError::DuplicateJob(dedupe_key_value)
      }
    }
    None => ()
  }
  let updated_job : @core.JobRecord = {
    ..job,
    status: @core.Pending,
    visible_at_ms: now_ms,
    finished_at_ms: None,
    lease_id: None,
    lease_owner: None,
    lease_deadline_ms: None,
    deadline_ms: None,
  }
  set_job(self, client, updated_job)
  ignore(
    get_int(
      client,
      [
        "ZADD",
        queue_visible_key(self, updated_job.queue_name),
        now_ms.to_string(),
        job_id,
      ],
      "ZADD retry dead",
    ),
  )
  true
}

///|
pub impl @backend.QueueBackend for RedisBackend with cancel_job(
  self,
  job_id,
  finished_at_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let job = match load_job(self, client, job_id) {
    Some(job) => job
    None => return false
  }
  match job.status {
    @core.Succeeded | @core.Dead | @core.Cancelled => false
    _ => {
      let updated_job : @core.JobRecord = {
        ..job,
        status: @core.Cancelled,
        finished_at_ms: Some(finished_at_ms),
        lease_id: None,
        lease_owner: None,
        lease_deadline_ms: None,
        deadline_ms: None,
      }
      set_job(self, client, updated_job)
      ignore(
        get_int(
          client,
          ["ZREM", queue_visible_key(self, job.queue_name), job_id],
          "ZREM cancel visible",
        ),
      )
      match job.lease_id {
        Some(lease_id) => {
          ignore(
            get_int(
              client,
              ["ZREM", leases_key(self), lease_id],
              "ZREM cancel lease",
            ),
          )
          ignore(
            get_int(
              client,
              ["DEL", lease_key(self, lease_id)],
              "DEL cancel lease",
            ),
          )
        }
        None => ()
      }
      release_dedupe(self, client, job.dedupe_key)
      true
    }
  }
}

///|
pub impl @backend.QueueBackend for RedisBackend with drain_queue(
  self,
  queue_name,
  finished_at_ms,
) raise {
  let client = open_client(self)
  defer client.close()
  let jobs = load_queue_jobs(self, client, queue_name)
  let mut drained = 0
  for job in jobs {
    match job.status {
      @core.Succeeded | @core.Dead | @core.Cancelled => ()
      _ => {
        let updated_job : @core.JobRecord = {
          ..job,
          status: @core.Cancelled,
          finished_at_ms: Some(finished_at_ms),
          lease_id: None,
          lease_owner: None,
          lease_deadline_ms: None,
          deadline_ms: None,
        }
        set_job(self, client, updated_job)
        ignore(
          get_int(
            client,
            ["ZREM", queue_visible_key(self, queue_name), job.job_id],
            "ZREM drain visible",
          ),
        )
        match job.lease_id {
          Some(lease_id) => {
            ignore(
              get_int(
                client,
                ["ZREM", leases_key(self), lease_id],
                "ZREM drain lease",
              ),
            )
            ignore(
              get_int(
                client,
                ["DEL", lease_key(self, lease_id)],
                "DEL drain lease",
              ),
            )
          }
          None => ()
        }
        release_dedupe(self, client, job.dedupe_key)
        drained = drained + 1
      }
    }
  }
  drained
}

///|
pub impl @backend.QueueBackend for RedisBackend with health(self) raise {
  let client = open_client(self)
  defer client.close()
  let pong = match client.ping() {
    Ok(pong) => pong
    Err(err) => raise raise_backend_failure("PING", err)
  }
  {
    ok: pong == "PONG",
    backend_kind: "redis",
    detail: Some("\{self.host}:\{self.port} prefix=\{self.prefix}"),
  }
}
