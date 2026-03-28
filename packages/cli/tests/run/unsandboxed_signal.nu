use ../../test.nu *

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		export default async function () {
			const process = await tg
				.spawn`sleep 1000sec`
				.stdin("null")
				.stdout("null")
				.stderr("null");
			await tg.sleep(0.1);
			await process.signal(tg.Process.Signal.TERM);
			const wait = await process.wait();
			return wait.exit;
		}
	',
}

let output = tg run $path | into int
assert ($output == 143)
