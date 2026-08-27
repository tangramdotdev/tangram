use ../../test.nu *

# Typing q in the fullscreen viewer exits successfully and cancels the process through its wait lease.

const driver = path self ../lib/pty_driver.py

if (which python3 | is-empty) {
	skip_test "this test requires python3"
}

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
	tg checkpoint watch process.wait.attach
	| from json
	| get watch
)
let pid_path = mktemp
let trigger_path = $pid_path + ".quit"
let tangram = which tg | where type == external | get path | first
let run = job spawn {
	let job_id = job id
	let output = (
		^python3 $driver $pid_path $trigger_path $tangram run --sandbox --stderr log --stdin null --stdout log --view fullscreen $path
		| complete
	)
	$output | job send --tag $job_id 0
}

wait_until {
	(open --raw $pid_path | str trim | str length) > 0
} "the PTY driver should publish the tg process ID"
let hit = tg checkpoint wait process.wait.attach $watch 0 | from json
let process = $hit.params.process
let sandbox = tg process get $process | from json | get sandbox

touch $trigger_path
tg checkpoint continue process.wait.attach $watch 0
tg checkpoint unwatch process.wait.attach $watch
let output = job recv --tag $run --timeout 10sec
success $output "q should exit the viewer successfully"

let outcome = tg wait $process | from json
assert equal $outcome.exit 1 "quitting the viewer should cancel the process"
assert ($outcome.error? | is-not-empty) "the cancelled process should have an error"
tg wait $sandbox
let state = tg sandbox get $sandbox | from json
assert equal $state.status "destroyed" "cancelling the process should destroy its sandbox"
