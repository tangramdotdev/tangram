use ../lib/test.nu *

# Cleaning fails when the server is not in single process mode.

let server = server spawn --config { advanced: { single_process: false } }

let output = tg clean | complete
failure $output
assert (
	($output.stderr | str contains 'cannot clean in multi-process mode')
	or ($output.stderr | str contains 'no indexers are available')
) $output.stderr
