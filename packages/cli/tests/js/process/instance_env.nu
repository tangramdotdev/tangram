use ../../../test.nu *

# A spawned process's env getter returns the full environment map, preserving value types.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
				env: { FOO: "bar", NUM: 42 },
			}).sandbox();
			return await process.env();
		}
	'
}

let output = tg build $path
snapshot $output '{"FOO":"bar","NUM":42}'
