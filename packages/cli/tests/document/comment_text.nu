use ../../test.nu *

# Documenting a module surfaces doc comment text for an export.

let server = spawn

let path = artifact {
	tangram.ts: '
		/** Greets the caller. */
		export function greet() { return "hi"; }
	'
}

let output = tg document $path | complete
success $output

let json = $output.stdout | from json
let entries = $json.exports.greet | values | flatten
assert equal $entries.0.comment.text "Greets the caller." "the doc comment text should appear in the documentation"
