use ../../test.nu *

# When a Promise.race settles, the losing branch is canceled, so its
# later side effects (here, a log line after a long sleep) never run.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await Promise.race([
				tg.sleep(0),
				f(),
			]);
		}

		async function f() {
			await tg.sleep(100);
			console.log("after sleep");
		}
	'
}
let id = tg build --detach $path
tg wait $id
let log = tg log $id
assert equal $log ''
