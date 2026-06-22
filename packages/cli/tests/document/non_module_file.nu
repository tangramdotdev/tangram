use ../../test.nu *

# Documenting a file that is not a module fails.

let server = spawn

let dir = mktemp --directory
let notes = $dir | path join notes.txt
"plain text" | save $notes

let output = tg document $notes | complete
failure $output
snapshot ($output.stderr | redact $notes) '
	error an error occurred
	-> expected a module path

'
