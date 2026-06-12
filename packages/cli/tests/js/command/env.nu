use ../../../test.nu *

# A command's env accessor returns its environment map.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg.command({
				host: "builtin",
				executable: "echo",
				env: { FOO: "bar" },
			});
			return await command.env;
		};
	'
}

let output = tg build $path
snapshot $output '{"FOO":"bar"}'
