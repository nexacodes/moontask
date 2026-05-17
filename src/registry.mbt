///|
struct TaskSpec {
  _id : Int
  _name : String
  retry : RetryPolicy
  run : async (TaskContext, Bytes) -> Unit
  _fallback : (async (TaskContext, Bytes, Error) -> Unit)?
}

///|
pub struct Registry {
  mut next_task_id : Int
  task_names : Array[String]
  specs : Map[String, TaskSpec]
}

///|
pub fn new_registry() -> Registry {
  { next_task_id: 1, task_names: [], specs: {} }
}

///|
pub fn Registry::contains(self : Registry, task_name : String) -> Bool {
  self.specs.contains(task_name)
}

///|
pub fn Registry::task_count(self : Registry) -> Int {
  self.specs.length()
}

///|
pub fn Registry::task_names(self : Registry) -> Array[String] {
  self.task_names.copy()
}

///|
pub fn[A] register_task(
  registry : Registry,
  options : TaskOptions[A],
) -> Task[A] raise {
  if registry.specs.contains(options.name) {
    raise QueueError::DuplicateTask(options.name)
  }
  let task_id = registry.next_task_id
  registry.next_task_id = task_id + 1
  let erased_fallback = match options.fallback {
    Some(fallback) =>
      Some(async fn(context : TaskContext, payload : Bytes, err : Error) {
        let decoded = (options.codec.decode)(payload)
        fallback(context, decoded, err)
      })
    None => None
  }
  let spec : TaskSpec = {
    _id: task_id,
    _name: options.name,
    retry: options.retry,
    run: async fn(context : TaskContext, payload : Bytes) {
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
    None => raise QueueError::TaskNotFound(task_name)
  }
}

///|
pub fn[A] max_attempts_for_task(task : Task[A], override_value : Int?) -> Int {
  match override_value {
    Some(limit) => limit
    None => retry_policy_max_attempts(task.retry)
  }
}

///|
pub async fn run_registered_job(
  registry : Registry,
  context : TaskContext,
  job : JobEnvelope,
) -> Unit {
  let spec = find_task_spec(registry, job.task_name)
  (spec.run)(context, job.payload)
}

///|
pub async fn run_registered_fallback(
  registry : Registry,
  context : TaskContext,
  job : JobEnvelope,
  err : Error,
) -> Unit {
  let spec = find_task_spec(registry, job.task_name)
  match spec._fallback {
    Some(fallback) => fallback(context, job.payload, err)
    None => ()
  }
}

///|
pub fn registry_retry_policy(
  registry : Registry,
  task_name : String,
) -> RetryPolicy raise {
  let spec = find_task_spec(registry, task_name)
  spec.retry
}
