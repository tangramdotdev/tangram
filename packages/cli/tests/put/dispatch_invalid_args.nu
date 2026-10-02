use ../lib/test.nu *

# Combining a process id with the kind flag is rejected because kind only applies to objects.

let local = server spawn

let output = tg put --no-tokens --id pcs_010000000000000000000000000000000000000000000000000000 --kind blb "x" | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> invalid args

'
