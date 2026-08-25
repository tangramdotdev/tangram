use ../../../test.nu *

# A command's user accessor returns its user.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({
				host: tg.host.current,
				executable: "echo",
				user: "nobody",
			});
			return await command.user;
		}
	'
}

let output = tg build $path
snapshot $output '"nobody"'
