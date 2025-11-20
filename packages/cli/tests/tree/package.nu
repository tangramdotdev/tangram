use ../../test.nu *

let server = spawn

# Create and tag dependencies.
let foo_path = artifact {
	tangram.ts: '// foo'
}
let output = tg tag foo $foo_path | complete
success $output

let bar_path = artifact {
	tangram.ts: 'import * as foo from "foo"'
}
let output = tg tag bar $bar_path | complete
success $output

# Create the main artifact.
let path = artifact {
	tangram.ts: '
		import * as foo from "foo";
		import * as bar from "bar";
	'
}

# Run tree command with package kind.
let output = tg tree $path --kind=package | complete
success $output

snapshot $output.stdout
