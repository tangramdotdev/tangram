use ../../util/test.nu *

# Waiting with --until for a status that the process already has returns at once, a finished process that started counts as started, and the status without --until prints the current status.

let local = server spawn

let path = artifact {
	tangram.ts: 'export default async function () { return 42; }',
}
let process = tg build --no-tokens --detach $path | referent node
tg wait $process | ignore

let output = tg process status $process | from json
assert equal $output [finished] "the status should be the current status"
let output = tg process status --until finished $process | from json
assert equal $output [finished] "waiting for the current status should return at once"
let output = tg process status --until started $process | from json
assert equal $output [finished] "waiting for a finished process that started to start should succeed"
