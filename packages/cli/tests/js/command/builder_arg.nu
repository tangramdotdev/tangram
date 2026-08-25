use ../../../test.nu *

# The builder's arg method appends a single argument.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg
				.command({ host: tg.host.current, executable: "echo" })
				.arg("x");
			return await command.args;
		}
	'
}

let output = tg build $path
snapshot $output '[{"kind":"string","value":"x"}]'
