use ../lib/test.nu *

# A process whose piped stdout is read on the runner's own server releases its control state once its reader has drained the pipe, instead of waiting for the process state time to live.

let server = server spawn --config {
	advanced: { checkpoints: true },
	runner: { process_state_ttl: 60, stdio_drained_ttl: 1 },
}
let watch = tg checkpoint watch runner.process.control.retention.finished | from json | get watch
let path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.run`echo hello`.sandbox().stdio("inherit");
		};
	',
}
let output = tg run --sandbox $path | complete
success $output
assert equal ($output.stdout | str trim) "hello"
success (timeout 20s tg checkpoint wait runner.process.control.retention.finished $watch 0 | complete) "the runner must release the process control state after the pipe is drained"
tg checkpoint unwatch runner.process.control.retention.finished $watch
