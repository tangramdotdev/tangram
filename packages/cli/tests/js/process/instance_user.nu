use ../../../test.nu *

# A spawned process exposes the user set on its command.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
				user: "nobody",
			}).sandbox();
			return await process.user;
		}
	'
}

let output = tg build $path
snapshot $output '"nobody"'
