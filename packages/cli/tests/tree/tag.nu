use ../../test.nu *

let server = spawn

# Create and tag dependencies.
let foo_path = artifact {
	tangram.ts: '// tree/of/tags/foo'
}
run tg tag tree/of/tags/foo $foo_path

let bar_path = artifact {
	tangram.ts: 'import * as foo from "tree/of/tags/foo"'
}
run tg tag tree/of/tags/bar $bar_path

# Run tree command with tag kind.
let output = run tg tree tree --kind=tag
snapshot $output
