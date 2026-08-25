use ../../../test.nu *

# tg.Process.expect returns the value unchanged when it is a process.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let process = await tg.spawn({
				host: tg.host.current,
				executable: "echo",
			}).sandbox();
			return tg.Process.expect(process) instanceof tg.Process;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
