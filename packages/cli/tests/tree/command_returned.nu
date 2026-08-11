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

let id = tg build --detach ($path | path join 'c')
let output = tg wait $id
snapshot $output '{"exit":0,"output":42}'

let output = tg view $id --mode inline --expand-processes --depth 1

snapshot $output '
	✓ fil_01ac4hnqyscdg2gjfdfevh5nkczpjv7cm4yj486xv3phpnrp9jv770#default
	├╴output: 42
	├╴command: cmd_01fcadwdpf6ps3nf7zv5vdncpv47h4j43pv28nwn2rzkpr06nxwgpg
	├╴✓ ../b.tg.ts#default
	└╴✓ fil_01bmpbckej87pxfjz87zeaht4sjyx2jw4jh3yvdqnr57bzygvt791g#default
'
