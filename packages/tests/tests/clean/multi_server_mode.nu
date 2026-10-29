use ../../util/test.nu *

# Cleaning fails when the server is in multi-server mode.

let local = server spawn --config { advanced: { multi_server: true } }

let output = tg clean | complete
failure $output
assert (
	($output.stderr | str contains 'cannot clean in multi-server mode')
	or ($output.stderr | str contains 'no indexers are available')
) $output.stderr
