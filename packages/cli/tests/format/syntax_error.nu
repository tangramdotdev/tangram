use ../../test.nu *

# Formatting a module with a syntax error fails and leaves the module unchanged.

let server = spawn

let dir = mktemp --directory
let contents = 'export default (((('
$contents | save ($dir | path join tangram.ts)

let output = tg format $dir | complete
failure $output
snapshot ($output.stderr | redact $dir) '
	error an error occurred
	-> failed to format
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to format
	-> failed to format the directory
	-> failed to format the path
	-> failed to format the file
	-> failed to format the module
	   path = <path>/tangram.ts
	-> failed to format the module
	-> Expected `)` but found `EOF`

'
assert equal (open ($dir | path join tangram.ts)) $contents "the module should be unchanged"
