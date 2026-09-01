use ../../test.nu *

# Disconnecting tg run before sandbox control connects can leave the accepted pre-wait sandbox running.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(60);
		}
	',
}

let watch = (
	tg checkpoint watch sandbox.control.connect
	| from json
	| get watch
)
let pid_path = mktemp
let tangram = which tg | where type == external | get path | first
let run = job spawn {
	let job_id = job id
	let output = (
		^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox "$3"' _ $pid_path $tangram $path
		| complete
	)
	$output | job send --tag $job_id 0
}

wait_until {
	(open --raw $pid_path | str trim | str length) > 0
} "tg run should publish its process ID"
let pid = open --raw $pid_path | str trim | into int
let hit = tg checkpoint wait sandbox.control.connect $watch 0 | from json
let sandbox = $hit.params.sandbox

kill --signal 15 $pid
let output = job recv --tag $run --timeout 10sec
assert equal $output.exit_code (-15) "the actual tg process should be terminated by SIGTERM"

tg checkpoint continue sandbox.control.connect $watch 0
tg checkpoint unwatch sandbox.control.connect $watch

wait_until {
	let output = tg sandbox get $sandbox | complete
	$output.exit_code == 0
} "the sandbox should connect after the checkpoint continues"
let state = tg sandbox get $sandbox | from json | get data
assert equal $state.status "started" "the sandbox should remain running without a wait lease"

# Clean up the accepted race window explicitly.
tg sandbox destroy $sandbox
tg wait $sandbox
let state = tg sandbox get $sandbox | from json | get data
assert equal $state.status "destroyed" "the characterized sandbox should be cleaned up"
