use ../../../test.nu *

# A spawned process exposes the working directory set on its command.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
				cwd: "/work",
			}).sandbox();
			return await process.cwd;
		}
	'
}

let output = tg build $path
snapshot $output '"/work"'
