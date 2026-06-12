use ../../../test.nu *

# A spawned sandboxed process has a string id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
			}).sandbox();
			return typeof process.id === "string";
		};
	'
}

let output = tg build $path
snapshot $output 'true'
