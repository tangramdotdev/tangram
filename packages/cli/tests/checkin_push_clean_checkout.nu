use ../test.nu *

# After pushing an object to a remote and cleaning it locally, checking out the object pulls it back from the remote, with snapshots matching on the local server, the remote, and the checkout.

let tmp = mktemp --directory

# Spawn a remote and local server.
let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create the artifact.
let artifact = '
	tg.file("Hello, World!")
'
let id = tg put $artifact
let output = tg object get --blobs --depth=inf --pretty $id
snapshot --name local $output

# Push.
tg push $id

# Remove the remote.
tg remote delete default

# Clean and confirm the object no longer exists.
tg clean
let output = tg get --blobs --depth=inf --pretty $id | complete
failure $output "should fail to get the object after clean"

# Confirm the object exists on the remote.
let output = tg --url $remote.url object get --blobs --depth=inf --pretty $id
snapshot --name remote $output

# Add the remote back.
tg remote put default $remote.url

# Check out the artifact, confirming it is pulled from the remote.
let path = $tmp | path join "output"
tg checkout $id $path
snapshot --name checkout --path $path
