use ../../test.nu *

# Getting the children of an object by a well formed id that does not exist fails.

let server = spawn

let output = tg object children fil_010000000000000000000000000000000000000000000000000000 | complete
failure $output
snapshot --normalize-ids $output.stderr r#'
	error an error occurred
	-> failed to get the object's children
	   id = fil_010000000000000000000000000000000000000000000000000000
	-> failed to load the object

'#
