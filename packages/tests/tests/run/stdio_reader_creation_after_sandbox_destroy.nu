use ../../util/test.nu *

# A process's piped stdout reaches the client when its stdout reader is created after the process exits.

let local = server spawn --config {
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

let reader_watch = (
	tg checkpoint watch runner.process.control.reader.create --params '{"stream":"stdout"}'
	| from json
	| get watch
)
let finish_watch = (
	tg checkpoint watch runner.process.finish
	| from json
	| get watch
)

let run = job spawn {
	let job_id = job id
	let output = tg run --sandbox $path | complete
	$output | job send --tag $job_id 0
}

# Hold stdout reader creation while allowing the process to exit.
tg checkpoint wait runner.process.control.reader.create $reader_watch 0 | ignore
tg checkpoint wait runner.process.finish $finish_watch 0 | ignore
tg checkpoint continue runner.process.finish $finish_watch 0
tg checkpoint unwatch runner.process.finish $finish_watch

# Release the stdout reader creation after the process has exited.
tg checkpoint continue runner.process.control.reader.create $reader_watch 0
tg checkpoint unwatch runner.process.control.reader.create $reader_watch

let output = job recv --tag $run --timeout 10sec
success $output
assert ($output.stdout | str contains "hello from the sandbox")
