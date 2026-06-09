use ../../test.nu *

# Formatting a path to a single module file rewrites it in place.

let server = spawn

let dir = mktemp --directory
'export default   "single"' | save ($dir | path join tangram.ts)

let output = tg format ($dir | path join tangram.ts) | complete
success $output
assert equal (open ($dir | path join tangram.ts)) "export default \"single\";\n" "the module should be formatted in place"
