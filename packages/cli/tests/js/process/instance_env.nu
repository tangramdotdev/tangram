use ../../../test.nu *

# A spawned process's env getter returns the full environment map, preserving value types.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
				env: { FOO: "bar", NUM: 42 },
			}).sandbox();
			return await process.env();
		}
	'
}

let output = tg build $path
snapshot $output '{"FOO":{"kind":"string","value":"bar"},"NUM":{"kind":"string","value":42}}'
