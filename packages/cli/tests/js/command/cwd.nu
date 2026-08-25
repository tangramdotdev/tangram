use ../../../test.nu *

# A command's cwd accessor returns its working directory.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({
				host: tg.host.current,
				executable: "echo",
				cwd: "/work",
			});
			return await command.cwd;
		}
	'
}

let output = tg build $path
snapshot $output '"/work"'
