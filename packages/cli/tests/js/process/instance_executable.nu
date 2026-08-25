use ../../../test.nu *

# A spawned process exposes its command's executable.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
			}).sandbox();
			return await process.executable;
		}
	'
}

let output = tg build $path
snapshot $output '{"artifact":null,"path":"echo"}'
