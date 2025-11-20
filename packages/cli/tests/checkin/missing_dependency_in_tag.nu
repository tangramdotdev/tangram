use ../../test.nu *

let server = spawn

# Tag foo which depends on bar (but bar does not exist).
let foo_path = artifact {
	tangram.ts: '
		import bar from "bar";
		export default () => bar();
	'
}
let tag_output = tg tag --no-solve foo $foo_path | complete
success $tag_output "the tag command should succeed with --no-solve"

# Try to check in a package that depends on foo.
let path = artifact {
	tangram.ts: '
		import foo from "foo";
		export default () => foo();
	'
}

let output = tg checkin $path | complete
failure $output "the command should fail when the dependency in the tag is missing"

let stderr = $output.stderr | str replace -a $path ''

snapshot -n stderr $stderr
