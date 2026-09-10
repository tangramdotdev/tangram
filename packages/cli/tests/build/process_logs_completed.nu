use ../../test.nu *

# A reader opened after the close notifications still reaches the stored end.
let server = server spawn --config { advanced: { checkpoints: true }, indexer: { log_compaction: false }, runner: { process_state_ttl: 0.01 } }
let watch = tg checkpoint watch runner.process.control.finished | from json | get watch
let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("stdout");
			console.error("stderr");
		}
	'
}
let id = tg build --detach $path | str trim
tg wait $id | ignore
timeout 10 tg checkpoint wait runner.process.control.finished $watch 0 | ignore
tg checkpoint continue runner.process.control.finished $watch 0
tg checkpoint unwatch runner.process.control.finished $watch

# Restart to discard every in-memory close notification.
let server = server restart $server
let output = timeout 10 tg log --no-timeout $id | complete
success $output
assert equal $output.stdout "stdout\n"
assert equal $output.stderr "stderr\n"
for stream in [stdout stderr] {
	let output = timeout 10 tg log --no-timeout --stream $stream $id | complete
	success $output
	assert equal ($output | get $stream) $"($stream)\n"
}
let output = timeout 10 tg log --no-timeout --position 100 $id | complete
success $output
assert equal $output.stdout ""
assert equal $output.stderr ""
