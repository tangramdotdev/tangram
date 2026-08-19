use ../../test.nu *

# Check-in is rejected when the server uses multi-directory storage.

let server = spawn --config {
	advanced: { single_directory: false }
}
let path = artifact 'Hello, World!'
let output = tg --url $server.url checkin $path | complete
failure $output
assert ($output.stderr | str contains 'check-in is not supported in multi-directory mode')
