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

# Extract only the cycle error message (the last error in the chain).
let output = $output
	| lines
	| str replace --all --regex 'fil_[a-z0-9]+' '<fil_id>'
	| str replace --all --regex 'cmd_[a-z0-9]+' 'cmd_id>'
snapshot $output '
	✓ <fil_id>#default
	├╴command: cmd_id>
	├╴output: 42
	├╴✓ ../b.tg.ts#default
	└╴✓ <fil_id>#default

'
