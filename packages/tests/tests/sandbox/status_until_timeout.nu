use ../../util/test.nu *

# Waiting with --until fails when the timeout passes before the sandbox has the status.

let local = server spawn

let sandbox = tg sandbox create --wait --no-tokens | referent node
let output = tg sandbox status --until stopped --timeout 500ms $sandbox | complete
failure $output "waiting past the timeout should fail"
assert ($output.stderr | str contains "the timeout passed before the sandbox had the status")
assert equal ($output.stdout | from json) [started] "the statuses should be the statuses observed before the timeout"
tg sandbox destroy $sandbox
