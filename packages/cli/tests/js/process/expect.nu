use ../../../test.nu *

# tg.Process.expect returns the value unchanged when it is a process.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn({
				host: "builtin",
				executable: "echo",
			}).sandbox();
			return tg.Process.expect(process) instanceof tg.Process;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
