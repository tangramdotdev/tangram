use ../../../test.nu *

# tg.Command.withId returns a command that preserves the given id.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({ host: tg.host.current, executable: "echo" });
			return tg.Command.withId(command.id).id === command.id;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
