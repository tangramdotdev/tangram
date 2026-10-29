use ../../util/test.nu *

# A sandbox whose runner task fails before it reports the sandbox's destruction is destroyed with the error that caused the failure.

let local = server spawn --config { advanced: { checkpoints: true } }

let sandbox = tg sandbox create --no-tokens | referent node

# Fail the runner's sandbox task during destruction, before it reports the destruction.
tg checkpoint panic runner.sandbox.capacity.release --params ({ sandbox: $sandbox } | to json)
tg sandbox destroy $sandbox

let output = timeout 10s tg sandbox wait $sandbox | complete
success $output
let outcome = $output.stdout | from json
assert equal $outcome.error.message "the sandbox task panicked" "waiting for the sandbox should return the error that caused the failure"

let data = tg sandbox get $sandbox | from json | get data
assert equal $data.status destroyed
