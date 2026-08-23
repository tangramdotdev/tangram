use ../../test.nu *

# Checking out an artifact by a well-formed id that does not exist fails.

let server = spawn

let path = ($env.TMPDIR? | default '/tmp') | path join 'checkout_nonexistent'

let output = tg checkout dir_0000000000000000000000000000 --path $path | complete
failure $output
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> failed to check out the artifact
	   artifact = dir_0000000000000000000000000000
	-> failed to ensure the artifact is available
	   artifact = dir_0000000000000000000000000000
	-> failed to find the artifact

'
