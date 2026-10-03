use ../lib/test.nu *

let local = server spawn
let path = artifact {
	tangram.ts: '
		export default async () => {
			const functionArg = await tg.Command.jsArg(child, []);
			tg.assert(functionArg.node.host === undefined);
			const shellArg = await tg.Process.spawnArg({ executable: "sh", args: ["-c", "true"], sandbox: true });
			tg.assert(shellArg.arg.command.node.host === undefined);
			const functionSpawn = await tg.Process.spawnArg({ command: functionArg, sandbox: true });
			tg.assert(functionSpawn.arg.command.node.host === undefined);
			const explicit = await tg.Process.spawnArg({ command: functionArg, host: "explicit-host", sandbox: true });
			tg.assert(explicit.arg.command.node.host === "explicit-host");
			const command = await tg.Command.js(child, []);
			const materialized = await tg.Process.spawnArg({ command, sandbox: true });
			tg.assert(materialized.arg.command.node.host === tg.host.current);
			return "ok";
		};
		export const child = () => "child";
	',
}
assert equal (tg --url $local.url build $path | from json) ok
