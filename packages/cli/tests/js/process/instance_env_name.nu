use ../../../test.nu *

# A spawned process's env getter returns a single value when given a name.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
				env: { FOO: "bar" },
			}).sandbox();
			return await process.env("FOO");
		};
	'
}

let output = tg build $path
snapshot $output '"bar"'
