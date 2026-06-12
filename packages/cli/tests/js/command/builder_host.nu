use ../../../test.nu *

# The builder's host method sets the host.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg
				.command({ host: "builtin", executable: "echo" })
				.host("other");
			return await command.host;
		};
	'
}

let output = tg build $path
snapshot $output '"other"'
