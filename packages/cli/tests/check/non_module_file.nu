use ../../test.nu *

# Checking a file that is not a module fails.

let server = spawn

let dir = mktemp --directory
let notes = $dir | path join notes.txt
"plain text" | save $notes

let output = tg check $notes | complete
failure $output
snapshot --normalize --redact $notes $output.stderr '
	error an error occurred
	-> expected a module path

'
