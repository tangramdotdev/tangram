use ../../../test.nu *

# An existing stored but unloaded command can be run unsandboxed.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({
				args: ["-c", "echo stored-unloaded"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
			await command.store();
			command.unload();
			let process = await tg.spawn(command).stdout("pipe");
			let [output, wait] = await Promise.all([
				process.stdout.text(),
				process.wait(),
			]);
			return { exit: wait.exit, output: output.trim() };
		}
	'
}

let output = tg build $path | from json
assert equal $output.exit 0
assert equal $output.output stored-unloaded
