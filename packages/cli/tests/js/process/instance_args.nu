use ../../../test.nu *

# A spawned process exposes its command's args through the args getter.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
				args: ["hi"],
			}).sandbox();
			return await process.args;
		}
	'
}

let output = tg build $path
snapshot $output '[{"kind":"string","value":"hi"}]'
