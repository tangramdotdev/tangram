use ../../util/test.nu *

# Stopping a sandbox finishes its running processes with an error.

let local = server spawn

let sandbox = tg sandbox create --no-tokens | referent node
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(120);
		}
	',
}
let process = tg run --no-tokens --detach $"--sandbox=($sandbox)" $path | referent node
tg sandbox stop $sandbox

let output = tg wait $process | from json
assert ($output.error? != null) "the process should finish with an error"
assert equal (tg sandbox get $sandbox | from json | get data.status) stopped
