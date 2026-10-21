use ../lib/test.nu *

# The processes health reflects a started process in the started count and the available capacity.

let local = server spawn --config {
	advanced: { checkpoints: true },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

let idle = tg health --fields processes | from json | get processes

let path = artifact {
	tangram.ts: '
		export default async function () {
			while (true) {
				await tg.sleep(1);
			}
		}
	'
}
let watch = tg checkpoint watch runner.process.start | from json | get watch
let build = tg build --detach --verbose $path | from json
tg checkpoint wait runner.process.start $watch 0 | ignore
tg checkpoint continue runner.process.start $watch 0
tg checkpoint unwatch runner.process.start $watch

let busy = tg health --fields processes | from json | get processes
assert equal $busy.started 1 "the started count should reflect the running process"
assert equal $busy.capacity.available.sandboxes ($idle.capacity.available.sandboxes - 1) "the running process should consume one sandbox"
if $nu.os-info.name == 'macos' {
	# A darwin sandbox has no resource bounds, so the running process reserves no CPU or memory.
	assert equal $busy.capacity.available.cpu.max $idle.capacity.available.cpu.max "a darwin sandbox should not reserve CPU"
	assert equal $busy.capacity.available.memory.max $idle.capacity.available.memory.max "a darwin sandbox should not reserve memory"
} else {
	assert equal $busy.capacity.available.cpu.max ($idle.capacity.available.cpu.max - 1) "the running process should consume one maximum CPU"
	assert equal $busy.capacity.available.memory.max ($idle.capacity.available.memory.max - 1_073_741_824) "the running process should consume one GiB of memory"
}

tg cancel $build.process $build.lease
tg wait $build.process
