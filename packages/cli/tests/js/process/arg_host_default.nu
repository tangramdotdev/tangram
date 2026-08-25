use ../../../test.nu *

# Sandboxed process arguments leave the host unset for the server to choose.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let object = await tg.Process.spawnArg({
				executable: "echo",
				sandbox: true,
			});
			let shorthand = await tg.Process.spawnArg("echo", {
				sandbox: true,
			});
			return [object, shorthand].every(({ arg }) =>
				typeof arg.command.node !== "string" && arg.command.node.host === undefined
			);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
