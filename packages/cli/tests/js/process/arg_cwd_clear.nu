use ../../../test.nu *

# Null clears an inherited command working directory during process argument composition.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: "builtin",
				executable: "echo",
				cwd: "/work",
			});
			let { arg } = await tg.Process.spawnArg(base, {
				cwd: null,
				sandbox: true,
			});
			let command = tg.Command.withId(arg.command.item);
			return (await command.cwd) === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
