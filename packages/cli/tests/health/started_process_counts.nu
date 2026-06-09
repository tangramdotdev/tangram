use ../../test.nu *

# The processes health reflects a started process in the started count and the available permits.

let server = spawn

let idle = tg health --fields processes | from json | get processes

let path = artifact {
	tangram.ts: '
		export default async () => {
			while (true) {
				await tg.sleep(1);
			}
		};
	'
}
let build = tg build --detach --verbose $path | from json
wait_until { (tg status --timeout 0 $build.process | from json) == ["started"] } "the process should start"

let busy = tg health --fields processes | from json | get processes
assert equal $busy.started 1 "the started count should reflect the running process"
assert equal $busy.permits ($idle.permits - 1) "the running process should consume one permit"

tg cancel $build.process $build.lease
tg wait $build.process
