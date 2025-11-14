use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		'tangram.ts': '
			import bar from "../bar";
			export default () => tg.run(bar);
		'
	}
	bar: {
		'tangram.ts': '
			export default () => tg.assert(false);
		'
	}
}

let output = tg run ($path | path join 'foo') | complete
assert not equal $output.exit_code 0
assert (snapshot $output.stdout)
