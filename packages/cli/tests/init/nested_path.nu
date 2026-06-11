use ../../test.nu *

# Initializing a nested path whose parent does not exist creates the full directory chain.

let dir = mktemp --directory
let path = $dir | path join "a" "b"

let output = tg init $path | complete
success $output

assert ($path | path exists) "the nested directory should be created"
assert (($path | path join tangram.ts) | path exists) "the scaffold should be created"
