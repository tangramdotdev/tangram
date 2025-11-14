use std assert
use ../../test.nu *

let server = spawn

# Tag foo which depends on bar (but bar does not exist).
let foo_path = artifact {
	'tangram.ts': '
		import bar from "bar";
		export default () => bar();
	'
}
let tag_output = tg tag --no-solve foo $foo_path | complete
assert ($tag_output.exit_code == 0) 'tag should succeed with --no-solve'

# Try to check in a package that depends on foo.
let path = artifact {
	'tangram.ts': '
		import foo from "foo";
		export default () => foo();
	'
}

let output = tg checkin $path | complete
assert not equal $output.exit_code 0 'should fail when dependency in tag is missing'

let stderr = $output.stderr | str replace -a $path ''

assert (snapshot -n stderr $stderr)
