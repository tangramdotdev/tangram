use ../../../test.nu *

# A spawned process exposes the user set on its command.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
				user: "nobody",
			}).sandbox();
			return await process.user;
		};
	'
}

let output = tg build $path
snapshot $output '"nobody"'
