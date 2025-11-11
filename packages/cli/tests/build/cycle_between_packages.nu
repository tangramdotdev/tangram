use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		'tangram.ts': '
			import bar from "../bar";
			export default () => tg.build(bar);
		'
	}
	bar: {
		'tangram.ts': '
			import foo from "../foo";
			export default () => tg.build(foo);
		'
	}
}

let output = tg build ($path | path join './foo') | complete
assert ($output.exit_code != 0)
