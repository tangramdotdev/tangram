use std assert
use ../../test.nu *

let server = spawn

# Create and tag the foo dependency with cyclic structure.
let foo_root = artifact {
	foo: {
		'tangram.ts': "
			import bar from \"../bar\";
			export default () => bar();
			export const failure = () => tg.assert(false, \"failure in foo\");
		"
	}
	bar: {
		'tangram.ts': "
			import { failure } from \"../foo\";
			export default () => failure();
		"
	}
}
tg tag foo ($foo_root | path join 'foo')

let path = artifact {
	'tangram.ts': "
		import foo from \"foo\";
		export default () => foo();
	"
}

let output = tg run ($path | path join 'tangram.ts') | complete
assert ($output.exit_code != 0)
assert (snapshot ($output.stderr | str trim))
