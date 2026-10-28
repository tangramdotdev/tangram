use ../../util/test.nu *

# Calling a process or command builder adds args to it.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export function echo(...args: Array<tg.Value>) { return args; }
		export default async function () {
			const command = (await tg.command(echo)) as tg.Command<Array<tg.Value>, tg.Value>;
			const process = await tg.build(command)("process");
			const builder = await tg.command(command)("command").build();
			return [process, builder];
		}
	'
}
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) [['process'] ['command']]
