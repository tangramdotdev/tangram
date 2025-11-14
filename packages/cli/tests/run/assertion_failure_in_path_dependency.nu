use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	foo: {
		'tangram.ts': '
			import foo from "../bar";
			export default () => foo();
		'
	}
	bar: {
		'tangram.ts': '
			export default () => tg.assert(false, "error")
		'
	}
}

let output = tg run ($path | path join 'foo/tangram.ts') | complete
failure $output
snapshot $output.stdout
