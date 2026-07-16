use ../../test.nu *

# Checking a module with a syntax error fails type checking.

let server = spawn

let dir = mktemp --directory
'export default ((((' | save ($dir | path join tangram.ts)

let output = tg check $dir | complete
failure $output
snapshot --normalize --redact $dir $output.stderr '
	error Expression expected.
	   ╭─[<redacted>/tangram.ts:1:20]
	 1 │ export default ((((
	   ╰────
	error an error occurred
	-> type checking failed

'
