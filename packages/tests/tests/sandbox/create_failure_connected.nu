use ../../util/test.nu *

# A sandbox whose creation fails on its runner after the sandbox is connected is destroyed with the error that caused the failure.

let local = server spawn

let sandbox = tg sandbox create --no-tokens --mount /tmp:/dev/target,ro | referent node

let output = timeout 10s tg sandbox wait $sandbox | complete
success $output
let outcome = $output.stdout | from json
assert equal $outcome.error.source.node.message "sandbox mounts at or beneath /dev, /proc, and /sys are not supported" "waiting for the sandbox should return the creation error"

let data = tg sandbox get $sandbox | from json | get data
assert equal $data.status destroyed
assert equal $data.error $outcome.error "the sandbox data should contain the same error"
