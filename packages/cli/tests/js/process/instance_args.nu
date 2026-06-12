use ../../../test.nu *

# A spawned process exposes its command's args through the args getter.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
				args: ["hi"],
			}).sandbox();
			return await process.args;
		};
	'
}

let output = tg build $path
snapshot $output '["hi"]'
