use ../../test.nu *

# Initializing a directory that already contains a root module fails.

let ts_dir = mktemp --directory
"" | save ($ts_dir | path join tangram.ts)
let output = tg init $ts_dir | complete
failure $output
snapshot ($output.stderr | redact $ts_dir) '
	error an error occurred
	-> found existing root module
	   module_path = <path>/tangram.ts

'

let js_dir = mktemp --directory
"" | save ($js_dir | path join tangram.js)
let output = tg init $js_dir | complete
failure $output
snapshot ($output.stderr | redact $js_dir) '
	error an error occurred
	-> found existing root module
	   module_path = <path>/tangram.js

'
