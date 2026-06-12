use ../../../test.nu *

# A command's cwd accessor returns its working directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg.command({
				host: "builtin",
				executable: "echo",
				cwd: "/work",
			});
			return await command.cwd;
		};
	'
}

let output = tg build $path
snapshot $output '"/work"'
