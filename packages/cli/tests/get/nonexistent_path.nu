use ../../test.nu *

# Getting a path whose parent does not exist fails to canonicalize, and getting a missing file in an existing directory fails to check in.

let server = spawn

let output = tg get /nonexistent/deeply/nested | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to canonicalize the path
	-> No such file or directory (os error 2)

'

let tmp = mktemp --directory
let output = tg get ($tmp | path join "nope") | complete
failure $output
snapshot --normalize --redact $tmp $output.stderr '
	error an error occurred
	-> failed to get the reference
	   reference = <redacted>/nope
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to get the reference
	   reference = <redacted>/nope
	-> failed to check in the path
	-> failed to find the root path
	-> failed to get the metadata
	   path = <redacted>/nope
	-> No such file or directory (os error 2)

'
