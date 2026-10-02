use ../lib/test.nu *

# Following a process status with --no-timeout streams observed statuses through to completion.

let local = server spawn

let path = artifact {
	tangram.ts: 'export default async function () { await tg.sleep(1); return 42; }',
}
let process = tg build --no-tokens --detach $path | referent node

let output = tg process status --no-timeout $process | from json
assert (($output | last) == "finished") "the status with follow should end with finished"
