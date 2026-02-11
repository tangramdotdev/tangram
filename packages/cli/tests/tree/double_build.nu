use ../../test.nu *
let server = spawn
let path = artifact {
	a.tg.ts: 'export default () => 42;',
	b.tg.ts: '
		import a from "./a.tg.ts";
		export default () => tg.resolve(a);
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

let id = tg build --host js -d ($path | path join 'c')
let output = tg wait $id
snapshot $output '{"exit":0,"output":42}'

let output = tg view $id --mode inline --expand-processes --depth 2

snapshot $output '
	✓ fil_01xw45e66hhhxemww9m1qmj7jp9n1zjhn3ewggx0f6eb9nwr4js46g#default
	├╴command: cmd_0166tgxrezqabf2zvd3mveyr2e1ss6ws1ehfeay2amxv5tadh47bhg
	├╴output: 42
	├╴✓ ../b.tg.ts#default
	└╴✓ fil_01sa3pyv7baf50x2ymmvy7p41zqnmmv8gp1fq5z3mq60ps8vcfxa30#default
'
