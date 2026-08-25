use ../../../test.nu *

# A command's host accessor returns its host.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({ host: tg.host.current, executable: "echo" });
			return (await command.host) === tg.host.current;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
