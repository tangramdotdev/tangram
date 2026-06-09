use ../../test.nu *

# Formatting a module with a syntax error fails and leaves the module unchanged.

let server = spawn

let dir = mktemp --directory
let contents = 'export default (((('
$contents | save ($dir | path join tangram.ts)

let output = tg format $dir | complete
failure $output
assert ($output.stderr | str contains "failed to format") "the error should mention the failed format"
assert equal (open ($dir | path join tangram.ts)) $contents "the module should be unchanged"
