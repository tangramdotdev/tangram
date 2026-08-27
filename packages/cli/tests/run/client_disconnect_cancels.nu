use ../../test.nu *

# Disconnecting tg run after its wait lease is attached cancels the process and destroys its sandbox.

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
let hit = tg checkpoint wait process.wait.attach $watch 0 | from json
let process = $hit.params.process
let sandbox = tg process get $process | from json | get sandbox

kill --signal 15 $pid
let output = job recv --tag $run --timeout 10sec
assert equal $output.exit_code (-15) "the actual tg process should be terminated by SIGTERM"

tg checkpoint continue process.wait.attach $watch 0
tg checkpoint unwatch process.wait.attach $watch

let outcome = tg wait $process | from json
assert equal $outcome.exit 1 "disconnecting the wait lease should cancel the process"
assert ($outcome.error? | is-not-empty) "the cancelled process should have an error"
tg wait $sandbox
let state = tg sandbox get $sandbox | from json
assert equal $state.status "destroyed" "cancelling the process should destroy its sandbox"
