use ../../test.nu *

# A process's sandbox must remain alive until its piped stdio is buffered.

let server = spawn --config {
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
let buffered_watch = (
	tg checkpoint watch runner.process.buffered
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
let destroyed = job spawn {
	let job_id = job id
	let output = tg checkpoint wait runner.sandbox.destroyed $destroyed_watch 0 | complete
	$output | job send --tag $job_id 0
}

# Hold stdout reader creation while allowing the process to exit.
tg checkpoint wait runner.process.control.reader.create $reader_watch 0 | ignore
tg checkpoint wait runner.process.finish $finish_watch 0 | ignore
tg checkpoint continue runner.process.finish $finish_watch 0
tg checkpoint unwatch runner.process.finish $finish_watch

# The sandbox cannot be destroyed before its stdout reader exists.
let early = try {
	job recv --tag $destroyed --timeout 250ms
} catch {
	null
}
assert ($early == null) "the sandbox must wait for its stdout reader"
tg checkpoint continue runner.process.control.reader.create $reader_watch 0
tg checkpoint unwatch runner.process.control.reader.create $reader_watch

# Hold the buffered event and confirm it gates sandbox destruction.
tg checkpoint wait runner.process.buffered $buffered_watch 0 | ignore
let early = try {
	job recv --tag $destroyed --timeout 250ms
} catch {
	null
}
assert ($early == null) "the sandbox must wait for the buffered event"
tg checkpoint continue runner.process.buffered $buffered_watch 0
tg checkpoint unwatch runner.process.buffered $buffered_watch

success (job recv --tag $destroyed --timeout 10sec)
tg checkpoint continue runner.sandbox.destroyed $destroyed_watch 0
tg checkpoint unwatch runner.sandbox.destroyed $destroyed_watch

let output = job recv --tag $run --timeout 10sec
success $output
assert ($output.stdout | str contains "hello from the sandbox")
