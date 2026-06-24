use ../../test.nu *

# The view command renders the expected process tree for a process that builds a command which itself builds another command.

let server = spawn
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

let id = tg build --host js --detach ($path | path join 'c')
let output = tg wait $id
snapshot $output '{"exit":0,"output":42}'

let output = tg view $id --mode inline --expand-processes --depth 1

snapshot $output '
	✓ fil_01f5g0qq7n6rnp2f4ya73vvnq2bvzsfgmvzkvd397ynqm4y1bnbt7g#default
	├╴output: 42
	├╴command: cmd_01nnd6msat28s73ja18tr6ded3med875bz0df6fcqqkzfrv8sdgpd0
	├╴✓ ../b.tg.ts#default
	└╴✓ fil_01bmpbckej87pxfjz87zeaht4sjyx2jw4jh3yvdqnr57bzygvt791g#default
'
