///|
pub(all) enum RedisValue {
  SimpleString(String)
  Error(String)
  Integer(Int)
  BulkString(String?)
  Array(Array[RedisValue])
} derive(Debug, Eq)

///|
pub type RedisString = Result[String, String]

///|
pub type RedisBulkString = Result[String?, String]

///|
pub type RedisInt = Result[Int, String]

///|
pub type RedisArray = Result[Array[String], String]

///|
pub type RedisHash = Result[Map[String, String], String]

///|
pub struct RedisClient {
  conn : @socket.Tcp
  pool : RedisClientPool?
  mut returned : Bool
  mut reusable : Bool
}

///|
pub(all) struct RedisClientPool {
  host : String
  port : Int
  max_size : Int
  idle_connections : @async.Queue[@socket.Tcp]
  create_lock : @async.Semaphore
  mut open_count : Int
}

///|
fn new_standalone_client(conn : @socket.Tcp) -> RedisClient {
  { conn, pool: None, returned: false, reusable: false }
}

///|
fn new_pooled_client(pool : RedisClientPool, conn : @socket.Tcp) -> RedisClient {
  { conn, pool: Some(pool), returned: false, reusable: true }
}

///|
fn command_payload(args : Array[String]) -> String {
  let buf = @buffer.new()
  buf.write_string("*\{args.length()}\r\n")
  for arg in args {
    let bytes = @utf8.encode(arg)
    buf.write_string("$\{bytes.length()}\r\n")
    buf.write_string(arg)
    buf.write_string("\r\n")
  }
  buf.to_string()
}

///|
fn read_closed_error() -> Error {
  @core.QueueError::BackendFailure("redis connection closed unexpectedly")
}

///|
async fn connect_transport(host : String, port : Int) -> @socket.Tcp {
  let conn = @socket.Tcp::connect_to_host(host, port~)
  conn.enable_keepalive()
  conn
}

///|
async fn RedisClient::read_line(self : RedisClient) -> String {
  match self.conn.read_until("\r\n") {
    Some(line) => line
    None => raise read_closed_error()
  }
}

///|
async fn RedisClient::drop_crlf(self : RedisClient) -> Unit {
  guard self.conn.drop(2) == 2 else { raise read_closed_error() }
}

///|
async fn RedisClient::read_bulk_string(self : RedisClient) -> RedisValue {
  let length_line = self.read_line()
  if length_line == "-1" {
    return BulkString(None)
  }
  let length = @string.parse_int(length_line) catch {
    _ =>
      raise @core.QueueError::BackendFailure(
        "invalid redis bulk string length: \{length_line}",
      )
  }
  let payload = self.conn.read_exactly(length) catch {
    _ => raise read_closed_error()
  }
  self.drop_crlf()
  BulkString(Some(@utf8.decode(payload)))
}

///|
async fn RedisClient::read_array(self : RedisClient) -> RedisValue {
  let count_line = self.read_line()
  if count_line == "-1" {
    return Array([])
  }
  let count = @string.parse_int(count_line) catch {
    _ =>
      raise @core.QueueError::BackendFailure(
        "invalid redis array length: \{count_line}",
      )
  }
  let values = []
  for _ in 0..<count {
    values.push(self.read_value())
  }
  Array(values)
}

///|
async fn RedisClient::read_value(self : RedisClient) -> RedisValue {
  let prefix_bytes = self.conn.read_exactly(1) catch {
    _ => raise read_closed_error()
  }
  let prefix = prefix_bytes[0].to_int()
  match prefix {
    43 => SimpleString(self.read_line())
    45 => Error(self.read_line())
    58 => {
      let value_line = self.read_line()
      let value = @string.parse_int(value_line) catch {
        _ =>
          raise @core.QueueError::BackendFailure(
            "invalid redis integer: \{value_line}",
          )
      }
      Integer(value)
    }
    36 => self.read_bulk_string()
    42 => self.read_array()
    _ =>
      raise @core.QueueError::BackendFailure(
        "unsupported redis response prefix: \{prefix}",
      )
  }
}

///|
pub async fn connect(host : String, port : Int) -> RedisClient {
  new_standalone_client(connect_transport(host, port))
}

///|
pub fn new_pool(
  host : String,
  port : Int,
  max_size? : Int = 32,
) -> RedisClientPool {
  guard max_size > 0 else { abort("redis client pool size must be positive") }
  {
    host,
    port,
    max_size,
    idle_connections: @async.Queue::new(kind=Unbounded),
    create_lock: @async.Semaphore::new(1),
    open_count: 0,
  }
}

///|
fn RedisClientPool::forget_connection(self : RedisClientPool) -> Unit {
  self.open_count = Int::max(0, self.open_count - 1)
}

///|
pub async fn RedisClientPool::acquire(self : RedisClientPool) -> RedisClient {
  match self.idle_connections.try_get() {
    Some(conn) => return new_pooled_client(self, conn)
    None => ()
  }
  self.create_lock.acquire()
  match self.idle_connections.try_get() {
    Some(conn) => {
      self.create_lock.release()
      return new_pooled_client(self, conn)
    }
    None => ()
  }
  if self.open_count < self.max_size {
    self.open_count = self.open_count + 1
    self.create_lock.release()
    let conn_result : Result[@socket.Tcp, Error] = try? connect_transport(
      self.host,
      self.port,
    )
    match conn_result {
      Ok(conn) => new_pooled_client(self, conn)
      Err(err) => {
        self.forget_connection()
        raise err
      }
    }
  } else {
    self.create_lock.release()
    new_pooled_client(self, self.idle_connections.get())
  }
}

///|
fn RedisClient::mark_broken(self : RedisClient) -> Unit {
  self.reusable = false
}

///|
pub fn RedisClient::close(self : RedisClient) -> Unit {
  guard !self.returned else { return }
  self.returned = true
  match self.pool {
    Some(pool) =>
      if self.reusable {
        ignore(try? pool.idle_connections.try_put(self.conn))
      } else {
        self.conn.close()
        pool.forget_connection()
      }
    None => self.conn.close()
  }
}

///|
pub async fn RedisClient::send(
  self : RedisClient,
  args : Array[String],
) -> RedisValue {
  try {
    self.conn.write(command_payload(args))
    self.read_value()
  } catch {
    err => {
      self.mark_broken()
      raise err
    }
  }
}

///|
pub fn RedisValue::to_bulk_string(self : RedisValue) -> RedisBulkString {
  match self {
    BulkString(value) => Ok(value)
    Error(err) => Err(err)
    _ => Err("Type mismatch: expected bulk string")
  }
}

///|
pub fn RedisValue::to_string(self : RedisValue) -> RedisString {
  match self {
    SimpleString(value) => Ok(value)
    BulkString(Some(value)) => Ok(value)
    Error(err) => Err(err)
    _ => Err("Type mismatch: expected string")
  }
}

///|
pub fn RedisValue::to_int(self : RedisValue) -> RedisInt {
  match self {
    Integer(value) => Ok(value)
    Error(err) => Err(err)
    _ => Err("Type mismatch: expected integer")
  }
}

///|
fn flatten_array_string(value : RedisValue) -> RedisString {
  match value {
    SimpleString(value) => Ok(value)
    BulkString(Some(value)) => Ok(value)
    Error(err) => Err(err)
    _ => Err("Type mismatch: expected string element")
  }
}

///|
pub fn RedisValue::to_array(self : RedisValue) -> RedisArray {
  match self {
    Array(values) => {
      let result = []
      for value in values {
        match flatten_array_string(value) {
          Ok(value) => result.push(value)
          Err(err) => return Err(err)
        }
      }
      Ok(result)
    }
    Error(err) => Err(err)
    _ => Err("Type mismatch: expected array")
  }
}

///|
pub fn RedisValue::to_hash(self : RedisValue) -> RedisHash {
  let arr = match self.to_array() {
    Ok(arr) => arr
    Err(err) => return Err(err)
  }
  guard arr.length() % 2 == 0 else {
    return Err("Type mismatch: expected even-sized hash array")
  }
  let pairs = {}
  let mut i = 0
  while i + 1 < arr.length() {
    pairs[arr[i]] = arr[i + 1]
    i = i + 2
  }
  Ok(pairs)
}

///|
pub async fn RedisClient::ping(self : RedisClient) -> RedisString {
  self.send(["PING"]).to_string()
}

///|
pub async fn RedisClient::set(
  self : RedisClient,
  key : String,
  value : String,
) -> RedisString {
  self.send(["SET", key, value]).to_string()
}

///|
pub async fn RedisClient::keys(
  self : RedisClient,
  pattern : String,
) -> RedisArray {
  self.send(["KEYS", pattern]).to_array()
}

///|
pub async fn RedisClient::incr(self : RedisClient, key : String) -> RedisInt {
  self.send(["INCR", key]).to_int()
}

///|
pub async fn RedisClient::smembers(
  self : RedisClient,
  key : String,
) -> RedisArray {
  self.send(["SMEMBERS", key]).to_array()
}

///|
pub async fn RedisClient::sadd(
  self : RedisClient,
  key : String,
  members : Array[String],
) -> RedisInt {
  self.send(["SADD", key, ..members]).to_int()
}

///|
pub async fn RedisClient::hgetall(
  self : RedisClient,
  key : String,
) -> RedisHash {
  self.send(["HGETALL", key]).to_hash()
}
