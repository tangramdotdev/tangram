use ../../lib/test.nu *

# Unsandboxed process arguments keep the command inline and leave the host unset for the server to choose.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let { arg } = await tg.Process.spawnArg({ executable: "echo" });
			return typeof arg.command.node !== "string" && arg.command.node.host === undefined;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
