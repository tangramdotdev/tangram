use ../../test.nu *

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			const host = tg.process.env.TANGRAM_HOST;
			tg.assert(typeof host === "string");
			const command = await tg.command({
				args: ["1000"],
				env: busybox(),
				executable: "sleep",
				host,
			});
			const process = await (tg.Process as any).spawn({
				checksum: undefined,
				command: { item: await command.store(), options: {} },
				create: false,
				mounts: [],
				network: false,
				parent: undefined,
				remote: undefined,
				retry: false,
				sandbox: false,
				stderr: "null",
				stdin: "null",
				stdout: "null",
			});
			await tg.sleep(0.1);
			await process.signal(tg.Process.Signal.TERM);
			const wait = await process.wait();
			return wait.exit;
		}
	',
}

let output = tg run $path | into int
assert ($output == 143)
