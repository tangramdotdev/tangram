use ../../../test.nu *

# A command's env accessor returns its environment map.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({
				host: tg.host.current,
				executable: "echo",
				env: { FOO: "bar" },
			});
			return await command.env;
		}
	'
}

let output = tg build $path
snapshot $output '{"FOO":{"kind":"string","value":"bar"}}'
