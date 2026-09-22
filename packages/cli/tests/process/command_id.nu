use ../lib/test.nu *

# A command ID stays an ID in process data and resolves to the same execution interface.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export const child = () => "done";
		export default async () => {
			const command = await tg.command(child);
			await command.store();
			const process = await tg.Process.spawnSandboxed({
				command: tg.Object.toReferent(command),
				sandbox: {},
				stderr: "log", stdin: "null", stdout: "log",
			});
			const output = await process.wait();
			tg.assert(output.exit === 0);
			const result = await process.command;
			tg.assert(result instanceof tg.Command && result.id === command.id);
			return { command: command.id, process: process.id };
		};
	'
}
let output = tg build $path | from json
let process = tg get --no-tokens $output.process | from json
assert equal $process.command $output.command
