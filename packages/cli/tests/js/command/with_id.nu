use ../../../test.nu *

# tg.Command.withId returns a command that preserves the given id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({ host: "builtin", executable: "echo" });
			return tg.Command.withId(command.id).id === command.id;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
