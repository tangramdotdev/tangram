use ../../../util/test.nu *

# Disposing a process handle releases its lease and cancels the process.

let local = server spawn --busybox --config { usage: true }

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			await using process = await tg.spawn`sleep 60`.env(tg.build(busybox)).sandbox();
			console.log(process.id);
			console.log(await process.sandbox);
		}
	',
}

let output = tg exec $path | complete
success $output
let lines = $output.stdout | lines
let process = $lines | get 0
let sandbox = $lines | get 1
assert ($process | str starts-with "pcs_")
assert ($sandbox | str starts-with "sbx_")
let outcome = tg wait $process | from json
assert equal $outcome.exit 1 "the process should be cancelled when its handle is disposed"
assert ($outcome.error? | is-not-empty) "the cancelled process should have an error"
tg wait $sandbox
let state = tg sandbox get $sandbox | from json | get data
assert equal $state.status "destroyed" "the sandbox should be destroyed with the process"

# A darwin sandbox reports no resource usage, so only a linux sandbox records CPU and memory time.
if $nu.os-info.name == 'linux' {
	assert ($state.usage.cpu.consumption > 0) "the sandbox should record actual CPU time"
	assert ($state.usage.memory.consumption > 0) "the sandbox should record actual memory time"
}
assert equal $state.ttl 0 "an implicit process sandbox should have a zero TTL"
