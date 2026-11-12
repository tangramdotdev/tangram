use ../../util/test.nu *

# Terminating one of two builds that share a process releases its lease without stopping the process, which the other build still holds.

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
let first = job spawn {
	let id = job id
	^bash -c 'echo "$$" > "$1"; exec "$2" build --view none "$3"' _ $pid_path $tangram $path
	| complete
	| job send --tag $id 0
}
let hit = tg checkpoint wait process.wait.attach $watch 0 | from json
tg checkpoint continue process.wait.attach $watch 0
tg checkpoint unwatch process.wait.attach $watch
let process = $hit.params.process

# Start a detached build of the same module, which shares the process.
let second = tg build --detach --verbose $path | from json
assert equal $second.process $process "the builds should share the process"

# Terminate the first build, which exits while the process keeps running for the second.
let pid = open --raw $pid_path | str trim | into int
kill --signal 15 $pid
let output = job recv --tag $first --timeout 10sec
assert equal $output.exit_code 143 "the build should exit with the code for the signal"
assert equal (tg process status --timeout 0 $process | from json) ["started"] "the process should still be running"
tg cancel $process $second.lease
