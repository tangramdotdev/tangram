use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': "
		import foo from \"./foo.tg.ts\";
		export default () => foo();
	"
	'foo.tg.ts': 'export default () => tg.assert(false);'
}

let output = tg run ($path | path join 'tangram.ts') | complete
assert ($output.exit_code != 0)
assert (snapshot ($output.stderr | str trim))
