use ../../../test.nu *

# A command's args accessor returns its argument list.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({
				host: tg.host.current,
				executable: "echo",
				args: ["a", "b"],
			});
			return await command.args;
		}
	'
}

let output = tg build $path
snapshot $output '[{"kind":"string","value":"a"},{"kind":"string","value":"b"}]'
