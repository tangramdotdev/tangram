use ../../util/test.nu *

# A sandbox create returns once a runner accepts the sandbox, without waiting for the scheduler's index write.

let server = server spawn --config { advanced: { checkpoints: true } }

# Hold the scheduler's index write, and create a sandbox.
let persist = checkpoint watch scheduler.sandbox.persist
let sandbox = tg sandbox create --no-tokens | referent node

# The create returned while the index write is still held.
checkpoint wait $persist | ignore

# The sandbox runs a process once the index write is released.
checkpoint unwatch $persist
let output = tg run $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'echo hello' | complete
success $output 'the process should run in the sandbox'
assert ($output.stdout | str contains 'hello')
