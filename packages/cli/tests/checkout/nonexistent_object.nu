use ../../test.nu *

# Checking out an artifact by a well-formed id that does not exist fails.

let server = spawn

let path = ($env.TMPDIR? | default '/tmp') | path join 'checkout_nonexistent'

let output = tg checkout dir_0000000000000000000000000000 $path | complete
failure $output
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> failed to check out the artifact
	   artifact = dir_0000000000000000000000000000
	-> failed to get the item
	-> failed to load the object

'
