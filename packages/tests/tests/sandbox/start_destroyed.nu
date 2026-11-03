use ../../util/test.nu *

# Starting a destroyed sandbox fails.

let local = server spawn

let sandbox = tg sandbox create --no-tokens | referent node
tg sandbox destroy $sandbox
tg sandbox wait $sandbox | ignore

let output = tg sandbox start $sandbox | complete
failure $output "starting a destroyed sandbox should fail"
assert ($output.stderr | str contains "the sandbox was destroyed")
