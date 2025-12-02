use ../../test.nu *

let server = spawn

# Create and tag the foo dependency.
let foo_path = artifact {
	tangram.ts: 'export default () => tg.file("foo");'
}
run tg tag foo/1.0.0 $foo_path

# Create and build the main artifact.
let artifact = artifact {
	tangram.ts: '
		import foo from "foo/*";
		export default () => tg.directory({ foo: foo() });
	'
}
let id = run tg build $artifact

let tmp = mktemp -d
let path = $tmp | path join "checkout"
run tg checkout --dependencies=true $id $path

# Clean.
run tg tag delete foo/1.0.0
run tg clean

let left = run tg checkin $path

assert equal $left $id
