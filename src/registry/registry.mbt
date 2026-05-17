///|
struct TaskSpec {
  _id : Int
  _name : String
  retry : @core.RetryPolicy
  run : async (@core.TaskContext, Bytes) -> Unit
  _fallback : (async (@core.TaskContext, Bytes, Error) -> Unit)?
}

///|
/// Mutable registry that maps task names to typed handlers and retry policies.
pub struct Registry {
  mut next_task_id : Int
  task_names : Array[String]
  specs : Map[String, TaskSpec]
}

///|
/// Creates an empty task registry.
pub fn new_registry() -> Registry {
  { next_task_id: 1, task_names: [], specs: {} }
}

///|
/// Returns whether a task name has already been registered.
pub fn Registry::contains(self : Registry, task_name : String) -> Bool {
  self.specs.contains(task_name)
}

///|
/// Returns the number of registered tasks.
pub fn Registry::task_count(self : Registry) -> Int {
  self.specs.length()
}

///|
/// Returns registered task names in registration order.
pub fn Registry::task_names(self : Registry) -> Array[String] {
  self.task_names.copy()
}

///|
/// Registers a typed task handler and returns the task handle for producers.
///
/// Raises `QueueError::DuplicateTask` when the task name is already present.
pub fn[A] register_task(
  registry : Registry,
  options : @core.TaskOptions[A],
) -> @core.Task[A] raise {
  if registry.specs.contains(options.name) {
    raise @core.QueueError::DuplicateTask(options.name)
  }
  let task_id = registry.next_task_id
  registry.next_task_id = task_id + 1
  let erased_fallback = match options.fallback {
    Some(fallback) =>
      Some(async fn(context : @core.TaskContext, payload : Bytes, err : Error) {
        let decoded = (options.codec.decode)(payload)
        fallback(context, decoded, err)
      })
    None => None
  }
  let spec : TaskSpec = {
    _id: task_id,
    _name: options.name,
    retry: options.retry,
    run: async fn(context : @core.TaskContext, payload : Bytes) {
      let decoded = (options.codec.decode)(payload)
      (options.handler)(context, decoded)
    },
    _fallback: erased_fallback,
  }
  registry.specs[options.name] = spec
  registry.task_names.push(options.name)
  {
    id: task_id,
    name: options.name,
    codec: options.codec,
    retry: options.retry,
  }
}

///|
fn find_task_spec(registry : Registry, task_name : String) -> TaskSpec raise {
  match registry.specs.get(task_name) {
    Some(spec) => spec
    None => raise @core.QueueError::TaskNotFound(task_name)
  }
}

///|
/// Computes the effective attempt limit for an enqueue operation.
pub fn[A] max_attempts_for_task(
  task : @core.Task[A],
  override_value : Int?,
) -> Int {
  match override_value {
    Some(limit) => limit
    None => @core.retry_policy_max_attempts(task.retry)
  }
}

///|
/// Decodes the stored payload and runs the registered handler for `job.task_name`.
pub async fn run_registered_job(
  registry : Registry,
  context : @core.TaskContext,
  job : @core.JobRecord,
) -> Unit {
  let spec = find_task_spec(registry, job.task_name)
  (spec.run)(context, job.payload)
}

///|
/// Runs the registered fallback for a job when one exists.
pub async fn run_registered_fallback(
  registry : Registry,
  context : @core.TaskContext,
  job : @core.JobRecord,
  err : Error,
) -> Unit {
  let spec = find_task_spec(registry, job.task_name)
  match spec._fallback {
    Some(fallback) => fallback(context, job.payload, err)
    None => ()
  }
}

///|
/// Loads the retry policy currently associated with a task name.
pub fn registry_retry_policy(
  registry : Registry,
  task_name : String,
) -> @core.RetryPolicy raise {
  let spec = find_task_spec(registry, task_name)
  spec.retry
}
