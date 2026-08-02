use ../../../test.nu *

# Null clears an inherited command working directory during process argument composition.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: tg.host.current,
				executable: "echo",
				cwd: "/work",
			});
			let { arg } = await tg.Process.spawnArg(base, {
				cwd: null,
				sandbox: true,
			});
			return typeof arg.command.item !== "string" && arg.command.item.cwd === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
