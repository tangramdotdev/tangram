use ../../../test.nu *

# A spawned process's env getter returns a single value when given a name.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
				env: { FOO: "bar" },
			}).sandbox();
			return await process.env("FOO");
		}
	'
}

let output = tg build $path
snapshot $output '{"kind":"string","value":"bar"}'
