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

let id = tg build -d ($path | path join 'c')
let output = tg wait $id
snapshot $output '{"exit":0,"output":42}'

let output = tg view $id --mode inline --expand-processes --depth 2

snapshot $output '
	✓ fil_01xw45e66hhhxemww9m1qmj7jp9n1zjhn3ewggx0f6eb9nwr4js46g#default
	├╴command: cmd_01wgr48erby6bfp12sd2vsr89ksk0zpj06djkr89kxsn6m75zckjk0
	├╴output: 42
	├╴✓ ../b.tg.ts#default
	└╴✓ ../a.tg.ts#default
'
