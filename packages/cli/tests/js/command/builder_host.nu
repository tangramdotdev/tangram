use ../../../test.nu *

# The builder's host method sets the host.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg
				.command({ host: tg.host.current, executable: "echo" })
				.host("x86_64-linux");
			return await command.host;
		}
	'
}

let output = tg build $path
snapshot $output '"x86_64-linux"'
