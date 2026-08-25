use ../../test.nu *

# A piped stdio read that arrives after the process's sandbox is destroyed receives the
# process's buffered output rather than failing to create the stream.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let path = artifact {
	tangram.ts: '
		export default () => {
			console.log("hello from the sandbox");
		};
	',
}

let read_watch = (
	tg checkpoint watch process.stdio.read.request --params '{"stream":"stdout"}'
	| from json
	| get watch
)
let destroyed_watch = (
	tg checkpoint watch runner.sandbox.destroyed
	| from json
	| get watch
)

let run = job spawn {
	let job_id = job id
	let output = tg run --sandbox $path | complete
	$output | job send --tag $job_id 0
}

# Hold the first stdout read until the process has exited and its sandbox is destroyed.
tg checkpoint wait process.stdio.read.request $read_watch 0 | ignore
tg checkpoint wait runner.sandbox.destroyed $destroyed_watch 0 | ignore
tg checkpoint continue runner.sandbox.destroyed $destroyed_watch 0
tg checkpoint unwatch runner.sandbox.destroyed $destroyed_watch
tg checkpoint continue process.stdio.read.request $read_watch 0
tg checkpoint unwatch process.stdio.read.request $read_watch

let output = job recv --tag $run --timeout 10sec
success $output
assert ($output.stdout | str contains "hello from the sandbox")
