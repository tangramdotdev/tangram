use ../../test.nu *

let server = spawn

# Create and tag dependencies.
let foo_1_0_0_path = artifact {
	tangram.ts: 'export default () => tg.file("foo 1.0.0");'
}
run tg tag foo/1.0.0 $foo_1_0_0_path

let foo_1_1_0_path = artifact {
	tangram.ts: 'export default () => tg.file("foo 1.1.0");'
}
run tg tag foo/1.1.0 $foo_1_1_0_path

let bar_path = artifact {
	tangram.ts: '
		import foo from "foo/^1";
		export default () => tg.directory({ foo: foo() });
	'
}
run tg tag bar $bar_path

# Create and build the main artifact.
let artifact = artifact {
	tangram.ts: '
		import foo from "foo/=1.0.0";
		import bar from "bar";
		export default () => tg.directory({ foo: foo(), bar: bar() });
	'
}
let id = run tg build $artifact

let tmp = mktemp -d
let path = $tmp | path join "checkout"
run tg checkout --dependencies=true $id $path

# Clean.
run tg tag delete foo/1.0.0
run tg tag delete foo/1.1.0
run tg tag delete bar
run tg clean

let left = run tg checkin $path

assert equal $left $id
