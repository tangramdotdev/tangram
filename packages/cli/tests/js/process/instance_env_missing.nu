use ../../../test.nu *

# A spawned process's env getter returns undefined for a name that is not set.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
			}).sandbox();
			return (await process.env("MISSING")) === undefined;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
