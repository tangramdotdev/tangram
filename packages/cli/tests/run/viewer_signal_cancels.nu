use ../../test.nu *

# SIGINT and SIGTERM gracefully exit a running viewer and cancel the process through its wait lease.

let server = spawn --config {
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
let tangram = which tg | where type == external | get path | first
for entry in ([
	{ exit: 130, signal: 2 },
	{ exit: 143, signal: 15 },
] | enumerate) {
	let case = $entry.item
	let pid_path = mktemp
	let run = job spawn {
		let job_id = job id
		let output = (
			^bash -c 'echo "$$" > "$1"; exec "$2" run --sandbox --view inline "$3"' _ $pid_path $tangram $path
			| complete
		)
		$output | job send --tag $job_id 0
	}

	wait_until {
		(open --raw $pid_path | str trim | str length) > 0
	} "tg run should publish its process ID"
	let pid = open --raw $pid_path | str trim | into int
	let hit = tg checkpoint wait process.wait.attach $watch $entry.index | from json
	let process = $hit.params.process
	let sandbox = tg process get $process | from json | get sandbox

	kill --signal $case.signal $pid
	let output = job recv --tag $run --timeout 10sec
	assert equal $output.exit_code $case.exit "the viewer should preserve the signal exit code"

	tg checkpoint continue process.wait.attach $watch $entry.index

	let outcome = tg wait $process | from json
	assert equal $outcome.exit 1 "exiting the viewer should cancel the process"
	assert ($outcome.error? | is-not-empty) "the cancelled process should have an error"
	let state = tg sandbox get $sandbox | from json
	assert equal $state.status "destroyed" "cancelling the process should destroy its sandbox"
}
tg checkpoint unwatch process.wait.attach $watch
