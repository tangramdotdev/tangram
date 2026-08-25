use ../../test.nu *

# The view command renders the expected process tree for a process that builds a command which itself builds another command.

let server = server spawn
let path = artifact {
	a.tg.ts: 'export default function () { return 42; }',
	b.tg.ts: '
		import a from "./a.tg.ts";
		export default function () { return tg.command(a); }
	',
	c: {
		tangram.ts: '
			import b from "../b.tg.ts";
			export default async function () {
				let command = await tg.build(b);
				return tg.build(command);
			}
		'
	}
}

let id = tg build --detach ($path | path join 'c')
let output = tg wait $id
snapshot $output '{"exit":0,"output":42}'

let output = tg view $id --mode inline --expand-processes --depth 1

snapshot --normalize-ids $output '
	✓ fil_010000000000000000000000000000000000000000000000000000#default
	├╴output: 42
	├╴command: cmd_010000000000000000000000000000000000000000000000000000
	├╴✓ ../b.tg.ts#default
	└╴✓ fil_011111111111111111111111111111111111111111111111111111#default
'
