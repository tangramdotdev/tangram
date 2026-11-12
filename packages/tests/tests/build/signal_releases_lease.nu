use ../../util/test.nu *

# Terminating the only build that holds a process releases its lease, which cancels the process, and the build exits with the code for the signal.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: '
		export default async function () {
			while (true) {
				await tg.sleep(1);
			}
		}
	'
}

# Start a build that waits for the process.
let watch = tg checkpoint watch process.wait.attach | from json | get watch
let pid_path = mktemp
let tangram = which tg | where type == external | get path | first
let build = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" build --view none "$3"' _ $pid_path $tangram $path
	| complete
	| job send --tag $id 0
}
let hit = tg checkpoint wait process.wait.attach $watch 0 | from json
tg checkpoint continue process.wait.attach $watch 0
tg checkpoint unwatch process.wait.attach $watch
let process = $hit.params.process

# Terminate the build, which exits without waiting for the process.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
let output = job recv --tag $build --timeout 10sec
assert equal $output.exit_code 143 "the build should exit with the code for the signal"

# The process is canceled, because no other build holds it.
let outcome = tg wait $process | from json
assert equal $outcome.exit 1 "releasing the only lease should cancel the process"
assert ($outcome.error? | is-not-empty) "the canceled process should have an error"
