use ../../test.nu *

# Create a remote server.
let remote = spawn --cloud -n remote

# Create a local server.
let local = spawn -n local

# Add the remote to the local server.
let output = tg remote put default $remote.url | complete
success $output

# Create a simple artifact.
let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("Hello, World!")
		}
	'
}

# Build the module.
let id = tg build $path

# First push - should push some objects.
let output = tg push $id | complete
success $output
snapshot $output.stderr '
	info pushed 0 processes, 2 objects, 58 B

'

# Index both sides to update stored state.
tg index
tg --url $remote.url index

# Second push without force - should push 0 objects because remote already has them.
let output = tg push $id | complete
success $output
snapshot $output.stderr '
	info pushed 0 processes, 0 objects, 0 B

'

# Third push with force - should push objects.
let output = tg push $id --force | complete
success $output
snapshot $output.stderr '
	info pushed 0 processes, 2 objects, 58 B

'
