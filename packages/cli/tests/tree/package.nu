use ../../test.nu *

let server = spawn

# Create and tag dependencies.
let foo_path = artifact {
	tangram.ts: '// foo'
}
tg tag foo $foo_path

let bar_path = artifact {
	tangram.ts: 'import * as foo from "foo"'
}
tg tag bar $bar_path

# Create the main artifact.
let path = artifact {
	tangram.ts: '
		import * as foo from "foo";
		import * as bar from "bar";
	'
}

# Run tree command with package kind.
let output = tg tree $path --kind=package
snapshot $output
