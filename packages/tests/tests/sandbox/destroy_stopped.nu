use ../../util/test.nu *

# Destroying a stopped sandbox destroys it, and it cannot start again.

let local = server spawn

let sandbox = tg sandbox create --no-tokens | referent node
tg sandbox stop $sandbox
tg sandbox destroy $sandbox

let output = tg sandbox wait $sandbox | complete
success $output "waiting for the destroyed sandbox should complete"
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed

let output = tg sandbox start $sandbox | complete
failure $output "starting a destroyed sandbox should fail"
