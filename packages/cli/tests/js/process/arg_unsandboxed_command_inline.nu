use ../../../test.nu *

# Unsandboxed process arguments keep the command inline instead of storing it.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let { arg } = await tg.Process.spawnArg({ executable: "echo" });
			return typeof arg.command.item !== "string" && arg.command.item.host === tg.host.current;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
