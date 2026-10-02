use ../lib/test.nu *

# Malformed mount and isolation options are rejected at argument parsing.

let local = server spawn

let output = tg sandbox create --no-tokens --mount "::bad::" | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value '::bad::' for '--mount <sandbox.mounts>': expected an absolute path

	For more information, try '--help'.

'#

let output = tg sandbox create --no-tokens --isolation warp | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value 'warp' for '--isolation <sandbox.isolation>': invalid isolation

	For more information, try '--help'.

'#
