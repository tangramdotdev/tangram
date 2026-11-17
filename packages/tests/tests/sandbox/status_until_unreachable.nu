use ../../util/test.nu *

# Waiting with --until for a status that the sandbox can no longer have fails, both when the sandbox is already destroyed and when it is destroyed during the wait.

let local = server spawn --config { advanced: { checkpoints: true } }

# Waiting for a stopped sandbox to start fails once the sandbox is destroyed.
let sandbox = tg sandbox create --wait --no-tokens | referent node
tg sandbox stop --wait $sandbox
let subscribed = checkpoint watch sandbox.status.subscribed --params { sandbox: $sandbox }
let status = job spawn {
	let id = job id
	tg sandbox status --until started $sandbox | complete | job send --tag $id 0
}
checkpoint pass $subscribed
checkpoint unwatch $subscribed
tg sandbox destroy $sandbox
let output = job recv --tag $status --timeout 60sec
failure $output "waiting for a destroyed sandbox to start should fail"
assert ($output.stderr | str contains "the sandbox can no longer have the status")
assert equal ($output.stdout | from json | first) stopped "the statuses should begin with stopped"
assert (($output.stdout | from json | last) in [destroying destroyed]) "the statuses should end once the sandbox is destroying"

# Waiting for a destroyed sandbox to start fails at once, and waiting for it to be destroyed succeeds.
tg sandbox wait $sandbox | ignore
let output = tg sandbox status --until started $sandbox | complete
failure $output "waiting for a destroyed sandbox to start should fail"
assert ($output.stderr | str contains "the sandbox can no longer have the status")
assert equal ($output.stdout | from json) [destroyed]
let output = tg sandbox status --until destroyed $sandbox | from json
assert equal $output [destroyed] "waiting for a destroyed sandbox to be destroyed should succeed"
