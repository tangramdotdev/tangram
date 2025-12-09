use ../../test.nu *

let server = spawn

# Create and tag the foo dependency.
let foo_path = artifact {
	tangram.ts: 'export default () => tg.file("foo");'
}
tg tag foo/1.0.0 $foo_path

# Create and build the main artifact.
let artifact = artifact {
	tangram.ts: '
		import foo from "foo/*";
		export default () => tg.directory({ foo: foo() });
	'
}
let id = tg build $artifact

let tmp = mktemp -d
let path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $path

# Clean.
tg tag delete foo/1.0.0
tg clean

let left = tg checkin $path

assert equal $left $id
