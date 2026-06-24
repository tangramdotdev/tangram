use ../../../test.nu *

# The builder's arg method appends a single argument.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg
				.command({ host: "builtin", executable: "echo" })
				.arg("x");
			return await command.args;
		}
	'
}

let output = tg build $path
snapshot $output '["x"]'
