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
	✓ fil_01xsrqantfb8nhj3548jm5dpt9ywkw6z0p0yv6y2mt1458t0pnx6ng#default
	├╴output: 42
	├╴command: cmd_01ks36mz40hqts0tbhandtzt25z8na9ftdd8p882ssdhsrs2st6js0
	├╴✓ ../b.tg.ts#default
	└╴✓ fil_01m7bw9grpp30bmdtac61mgg2sp9kd2nqz6qpn6a8fhs594h9n7bbg#default
'
