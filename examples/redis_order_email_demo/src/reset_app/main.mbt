///|
let help_message : String =
  #|Usage: moon run src/reset_app
  #|
  #|Delete all Redis keys under prefix:
  #|  \{@demo.redis_prefix}

///|
async fn main {
  match @env.args()[1:] {
    ["-h" | "--help", ..] => {
      println(help_message)
      return
    }
    [['-', ..] as opt, ..] => fail("unknown option `\{opt}`")
    [arg, ..] => fail("unexpected positional argument `\{arg}`")
    [] => ()
  }
  let deleted = try! @demo.reset_demo_keys()
  println(
    "deleted \{deleted} keys from redis prefix=\{@demo.redis_prefix} host=\{@demo.redis_host} port=\{@demo.redis_port}",
  )
}
