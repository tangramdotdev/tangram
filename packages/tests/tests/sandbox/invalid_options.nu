use ../../util/test.nu *

# Malformed mount and isolation options are rejected at argument parsing.

let local = server spawn

let output = tg sandbox create --no-tokens --mount "::bad::" | complete
failure $output
snapshot --normalize $output.stderr r#'
	error: invalid value '::bad::' for '--mount <sandbox.mounts>': expected an absolute path

	For more information, try '--help'.

'#

# Backend selection is internal and cannot be requested through sandbox creation.
let output = tg sandbox create --no-tokens --isolation warp | complete
failure $output
assert ($output.stderr | str contains "unexpected argument '--isolation'")
