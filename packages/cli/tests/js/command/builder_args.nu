use ../../../test.nu *

# The builder's args method appends an array of arguments.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg
				.command({ host: tg.host.current, executable: "echo" })
				.args(["y", "z"]);
			return await command.args;
		}
	'
}

let output = tg build $path
snapshot $output '[{"kind":"string","value":"y"},{"kind":"string","value":"z"}]'
