use ../../test.nu *

# Formatting a path to a file that is not a module succeeds without changing the file.

let server = spawn

let dir = mktemp --directory
let contents = 'export default   "plain"'
$contents | save ($dir | path join plain.ts)

let output = tg format ($dir | path join plain.ts) | complete
success $output
assert equal (open ($dir | path join plain.ts)) $contents "the file should be unchanged"
