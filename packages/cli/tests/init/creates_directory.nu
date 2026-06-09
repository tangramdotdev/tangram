use ../../test.nu *

# Initializing a path that does not exist creates the directory.

let dir = mktemp --directory
let path = $dir | path join "package"

let output = tg init $path | complete
success $output

assert ($path | path exists) "the directory should be created"
assert (($path | path join tangram.ts) | path exists) "the scaffold should be created"
