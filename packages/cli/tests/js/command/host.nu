use ../../../test.nu *

# A command's host accessor returns its host.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg.command({ host: "builtin", executable: "echo" });
			return await command.host;
		};
	'
}

let output = tg build $path
snapshot $output '"builtin"'
