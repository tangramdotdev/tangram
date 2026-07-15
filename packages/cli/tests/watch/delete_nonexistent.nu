use ../../test.nu *

# Deleting a watch on a path that has no watch fails with a missing-watch error.

let server = spawn

let path = artifact 'test'

let output = tg watch delete $path | complete
failure $output
snapshot --normalize --redact $path $output.stderr '
	error an error occurred
	-> failed to delete the watch
	   path = <redacted>
	-> failed to find the watch

'
