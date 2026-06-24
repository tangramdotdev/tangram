use ../../../test.nu *

# A spawned process exposes its underlying command through the command getter.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
			}).sandbox();
			return (await process.command) instanceof tg.Command;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
