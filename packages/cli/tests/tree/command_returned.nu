use ../../test.nu *

# The view command renders the expected process tree for a process that builds a command which itself builds another command.

let server = spawn
let path = artifact {
	a.tg.ts: 'export default () => 42;',
	b.tg.ts: '
		import a from "./a.tg.ts";
		export default () => tg.command(a);
	',
	c: {
		tangram.ts: '
			import b from "../b.tg.ts";
			export default async () => {
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
	✓ fil_01g1t9dmfw9v9arvs2k8e3x6zt69gpx993gbw8tw5jgpzqzptxt8fg#default
	├╴output: 42
	├╴command: cmd_01wpp1n9dh6nvk8h4m4jjsj7687dcn9wm12qm9dd2tqb5kfe2mkms0
	├╴✓ ../b.tg.ts#default
	└╴✓ fil_01sa3pyv7baf50x2ymmvy7p41zqnmmv8gp1fq5z3mq60ps8vcfxa30#default
'
