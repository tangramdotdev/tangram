use ../../../test.nu *

# The builder's args method appends an array of arguments.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg
				.command({ host: "builtin", executable: "echo" })
				.args(["y", "z"]);
			return await command.args;
		};
	'
}

let output = tg build $path
snapshot $output '["y","z"]'
