use ../lib/test.nu *

# Calling process.signal with TERM on a running unsandboxed process terminates it and wait reports the corresponding exit status.

let local = server spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			const process = await tg.spawn`
				sleep 1000
			`
				.env(tg.build(busybox))
				.stderr("null")
				.stdin("null")
				.stdout("null");
			await tg.sleep(0.1);
			await process.signal(tg.Process.Signal.TERM);
			const outcome = await process.wait();
			return outcome.exit;
		}
	',
}

let output = tg run $path | into int
assert ($output == 143)
