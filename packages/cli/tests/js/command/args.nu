use ../../../test.nu *

# A command's args accessor returns its argument list.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg.command({
				host: "builtin",
				executable: "echo",
				args: ["a", "b"],
			});
			return await command.args;
		};
	'
}

let output = tg build $path
snapshot $output '["a","b"]'
