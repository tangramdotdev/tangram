use ../../test.nu *

# Downloading a URL that responds with an error status fails with the reason on the CLI.

skip_if_offline

let server = spawn

let output = tg download "http://www.example.com/does-not-exist" --checksum sha256:any | complete
failure $output
snapshot --normalize $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> expected a success status
	   url = http://www.example.com/does-not-exist
	-> HTTP status client error (404 Not Found) for url (http://www.example.com/does-not-exist)

'
