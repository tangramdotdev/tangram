use ../../../test.nu *

# A command's stdin accessor returns the blob set as standard input.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg.command({
				host: "builtin",
				executable: "cat",
				stdin: await tg.blob("input data"),
			});
			let stdin = await command.stdin;
			return await stdin.text;
		};
	'
}

let output = tg build $path
snapshot $output '"input data"'
