use ../../util/test.nu *

# Destroying a sandbox while its process is starting cancels the process.

let server = server spawn --config { advanced: { checkpoints: true } }
let sandbox = tg sandbox create --no-tokens | referent node

# Hold the process before it is spawned in the sandbox.
let start_watch = tg checkpoint watch runner.process.start | from json | get watch
let shutdown_watch = tg checkpoint watch runner.sandbox.shutdown | from json | get watch
let process = tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'sleep 1000' | referent node
tg checkpoint wait --timeout 30s runner.process.start $start_watch 0 | ignore

# Destroy the sandbox, and release the process after the sandbox's server shuts down.
tg sandbox destroy $sandbox
tg checkpoint wait --timeout 30s runner.sandbox.shutdown $shutdown_watch 0 | ignore
tg checkpoint continue runner.sandbox.shutdown $shutdown_watch 0
tg checkpoint unwatch runner.sandbox.shutdown $shutdown_watch
tg checkpoint continue runner.process.start $start_watch 0
tg checkpoint unwatch runner.process.start $start_watch

# The process reports the cancellation.
let output = tg wait $process | from json
assert equal $output.exit 1
assert ((tg get $output.error) | str contains '"code":"cancellation"') 'the process should report the cancellation'
