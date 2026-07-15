use ../../test.nu *

# The processes health reflects a started process in the started count and the available capacity.

let server = spawn

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
let build = tg build --detach --verbose $path | from json
wait_until { (tg status --timeout 0 $build.process | from json) == ["started"] } "the process should start"

let busy = tg health --fields processes | from json | get processes
assert equal $busy.started 1 "the started count should reflect the running process"
assert equal $busy.capacity.available.cpus ($idle.capacity.available.cpus - 1) "the running process should consume one CPU"
assert equal $busy.capacity.available.memory ($idle.capacity.available.memory - (1e9 | into int)) "the running process should consume one GB of memory"

tg cancel $build.process $build.lease
tg wait $build.process
