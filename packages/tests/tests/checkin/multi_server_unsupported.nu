use ../../lib/test.nu *

# Check-in is rejected in multi-server mode.

let local = server spawn --config {
	advanced: { multi_server: true }
}
let path = artifact 'Hello, World!'
let output = tg --url $local.url checkin --no-tokens $path | complete
failure $output
assert ($output.stderr | str contains 'check-in is not supported in multi-server mode')
