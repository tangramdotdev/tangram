use ../lib/test.nu *

# An inline command executes without storing its canonical command object.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export const child = () => "done";
		export default async () => {
			const command = await tg.command(child);
			const process = await tg.spawn(command).sandbox();
			const output = await process.wait();
			tg.assert(output.exit === 0);
			const data = await process.command;
			tg.assert(!(data instanceof tg.Command));
			tg.assert(data.host === tg.host.current);
			return { command: command.id, process: process.id };
		};
	'
}

let output = tg build $path | from json
let process = tg get $output.process | from json
assert (($process.command.node | describe) | str starts-with record)
failure (tg get $output.command | complete) "the inline command object must not be stored"
