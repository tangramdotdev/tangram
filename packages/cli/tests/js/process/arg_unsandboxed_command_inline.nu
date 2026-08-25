use ../../../test.nu *

# Unsandboxed process arguments keep the command inline instead of storing it.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let { arg } = await tg.Process.spawnArg({ executable: "echo" });
			return typeof arg.command.node !== "string" && arg.command.node.host === tg.host.current;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
