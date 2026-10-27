use ../lib/test.nu *

# A sandbox whose process exits after it is created is destroyed with the error that describes the exit.

let local = server spawn

let sandbox = tg sandbox create --no-tokens --mount /nonexistent:/target,ro | referent node

let output = timeout 10s tg sandbox wait $sandbox | complete
success $output
let outcome = $output.stdout | from json
assert ($outcome.error.message | str contains "the sandbox process exited") "waiting for the sandbox should return the error that describes its exit"

let data = tg sandbox get $sandbox | from json | get data
assert equal $data.status destroyed
assert equal $data.error $outcome.error "the sandbox data should contain the same error"
