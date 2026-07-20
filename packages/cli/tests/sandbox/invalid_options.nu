use ../../test.nu *

# Malformed mount and isolation options are rejected at argument parsing.

let server = spawn

let output = tg sandbox create --mount "::bad::" | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value '::bad::' for '--mount <sandbox.mounts>': expected an absolute path
	
	For more information, try '--help'.

'#

let output = tg sandbox create --isolation warp | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value 'warp' for '--isolation <sandbox.isolation>': invalid isolation
	
	For more information, try '--help'.

'#
