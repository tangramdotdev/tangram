use ../../test.nu *

# Create a remote server.
let remote = spawn --cloud -n remote

# Create a local server.
let local = spawn -n local

# Add the remote to the local server.
let output = tg remote put default $remote.url | complete
success $output

# Create a simple process.
let path = artifact {
	tangram.ts: '
		export default () => {
			return 5
		}
	'
}
let process_id = tg build -d $path | str trim
tg wait $process_id

# First push - should push the process.
let output = tg --no-quiet push --commands --no-outputs $process_id | complete
success $output
assert ($output.stderr | str contains "transferred 1 processes") "expected the first push to transfer the process"

# Index both sides.
tg index
tg --url $remote.url index

# Second push without force - should not transfer the indexed process.
let output = tg --no-quiet push --commands --no-outputs $process_id | complete
success $output
assert ($output.stderr | str contains "transferred 0 processes") "expected the second push not to transfer the process"

# Third push with force - should transfer the process again.
let output = tg --no-quiet push --force --commands --no-outputs $process_id | complete
success $output
assert ($output.stderr | str contains "transferred 1 processes") "expected the forced push to transfer the process"
