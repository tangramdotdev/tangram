use ../../test.nu *

# Initializing a directory that already contains a root module fails.

let ts_dir = mktemp --directory
"" | save ($ts_dir | path join tangram.ts)
let output = tg init $ts_dir | complete
failure $output
assert ($output.stderr | str contains "found existing root module") "the error should mention the existing root module"

let js_dir = mktemp --directory
"" | save ($js_dir | path join tangram.js)
let output = tg init $js_dir | complete
failure $output
assert ($output.stderr | str contains "found existing root module") "the error should mention the existing root module"
