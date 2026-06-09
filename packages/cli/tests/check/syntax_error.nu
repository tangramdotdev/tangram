use ../../test.nu *

# Checking a module with a syntax error fails type checking.

let server = spawn

let dir = mktemp --directory
'export default ((((' | save ($dir | path join tangram.ts)

let output = tg check $dir | complete
failure $output
assert ($output.stderr | str contains "type checking failed") "the error should mention the failed type checking"
