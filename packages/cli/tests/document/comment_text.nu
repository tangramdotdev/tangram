use ../lib/test.nu *

# Documenting a module surfaces doc comment text for an export.

let local = server spawn

let path = artifact {
	tangram.ts: '
		/** Greets the caller. */
		export function greet() { return "hi"; }
	'
}

let output = tg document $path | complete
success $output

let json = $output.stdout | from json
let declaration = $json.declarations | get ($json.exports.greet | first)
assert equal $declaration.comment.text "Greets the caller." "the doc comment text should appear in the documentation"
