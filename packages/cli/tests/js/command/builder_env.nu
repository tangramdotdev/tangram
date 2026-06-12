use ../../../test.nu *

# The builder's env method sets environment variables.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg
				.command({ host: "builtin", executable: "echo" })
				.env({ K: "v" });
			return await command.env;
		};
	'
}

let output = tg build $path
snapshot $output '{"K":"v"}'
