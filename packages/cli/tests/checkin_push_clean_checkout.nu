use std assert
use ../test.nu *

# Spawn a remote and local server.
let remote = spawn
let local = spawn {
	remotes: [{ name: default, url: $remote.url }]
}

# Check in the artifact.
let path = artifact {
	'tangram.ts': '
		export default () => "Hello, World!";
	'
}
let id = tg checkin $path
let output = tg object get --blobs --depth=inf --pretty $id
assert (snapshot -n local $output)

# Push.
tg push $id

# Remove the remote.
tg remote delete default

# Clean and confirm the object no longer exists.
tg clean
let output = tg get --blobs --depth=inf --pretty $id | complete
assert not equal $output.exit_code 0 'should failed to get the object after clean'

# Confirm the object exists on the remote.
let output = tg -u $remote.url object get --blobs --depth=inf --pretty $id
assert (snapshot -n remote $output)

# Add the remote back.
tg remote put default $remote.url

# Check out the artifact, confirming it is pulled from the remote.
let path = (mktemp -d) | path join "output"
tg checkout $id $path
assert (snapshot -n checkout --path $path)
