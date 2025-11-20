use ../../test.nu *

let server = spawn

# Create and tag dependencies.
let foo_path = artifact {
	tangram.ts: '// tree/of/tags/foo'
}
let output = tg tag tree/of/tags/foo $foo_path | complete
success $output

let bar_path = artifact {
	tangram.ts: 'import * as foo from "tree/of/tags/foo"'
}
let output = tg tag tree/of/tags/bar $bar_path | complete
success $output

# Run tree command with tag kind.
let output = tg tree tree --kind=tag | complete
success $output

snapshot $output.stdout
