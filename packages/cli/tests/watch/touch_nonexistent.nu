use ../../test.nu *

# Touching a watch on a path that has no watch fails.

let server = spawn

let path = artifact 'test'

let output = tg watch touch $path | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error an error occurred
	-> failed to touch the watch
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to touch the watch
	-> expected a watch
	   path = <redacted>

'
