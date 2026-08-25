use ../../../test.nu *

# The builder's env method sets environment variables.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg
				.command({ host: tg.host.current, executable: "echo" })
				.env({ K: "v" });
			return await command.env;
		}
	'
}

let output = tg build $path
snapshot $output '{"K":{"kind":"string","value":"v"}}'
