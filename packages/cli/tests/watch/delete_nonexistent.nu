use ../../test.nu *

# Deleting a watch on a path that has no watch fails with a missing-watch error.

let server = spawn

let path = artifact 'test'

let output = tg watch delete $path | complete
failure $output
snapshot ($output.stderr | redact $path) '
	error an error occurred
	-> failed to delete the watch
	   path = <path>
	-> failed to find the watch

'
