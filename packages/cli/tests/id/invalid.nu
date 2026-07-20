use ../../test.nu *

# Input that is not an id in either text or binary form is rejected.

let output = tg id "not_an_id" | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> invalid id

'

let output = "" | tg id | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> invalid id

'
