use ../../test.nu *

# Two packages with a cyclic import graph build successfully as long as their processes do not form a cycle.

let server = spawn

let path = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default function () { return tg.build(bar); }
			export function greeting() { return "foo"; }
		'
	}
	bar: {
		tangram.ts: '
			import * as foo from "../foo";
			export default function () { return tg.build(foo.greeting); }
		'
	}
}

let output = tg build ($path | path join './foo')
snapshot $output
