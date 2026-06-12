use ../../../test.nu *

# A spawned process exposes its command's executable.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
			}).sandbox();
			return await process.executable;
		};
	'
}

let output = tg build $path
snapshot $output '{"path":"echo"}'
