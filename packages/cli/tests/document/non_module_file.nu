use ../../test.nu *

# Documenting a file that is not a module fails.

let server = spawn

let dir = mktemp --directory
"plain text" | save ($dir | path join notes.txt)

let output = tg document ($dir | path join notes.txt) | complete
failure $output
assert ($output.stderr | str contains "expected a module path") "the error should mention the module path"
