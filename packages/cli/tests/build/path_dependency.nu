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
		'tangram.ts': 'export default () => "Hello from bar";'
	}
}

let output = tg build ($path | path join './foo') | complete
assert equal $output.exit_code 0
assert (snapshot ($output.stdout | str trim))
