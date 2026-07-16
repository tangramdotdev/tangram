use ../../test.nu *

# Initializing a directory that already contains a root module fails.

let ts_dir = mktemp --directory
"" | save ($ts_dir | path join tangram.ts)
let output = tg init $ts_dir | complete
failure $output
snapshot --normalize --redact $ts_dir $output.stderr '
	error an error occurred
	-> found existing root module
	   module_path = <redacted>/tangram.ts

'

let js_dir = mktemp --directory
"" | save ($js_dir | path join tangram.js)
let output = tg init $js_dir | complete
failure $output
snapshot --normalize --redact $js_dir $output.stderr '
	error an error occurred
	-> found existing root module
	   module_path = <redacted>/tangram.js

'
