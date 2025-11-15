use ../test.nu *

let tmp = mktemp -d

# Spawn a remote and local server.
let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Check in the artifact.
let path = artifact {
	tangram.ts: '
		export default () => "Hello, World!";
	'
}
let id = run tg checkin $path
let output = run tg object get --blobs --depth=inf --pretty $id
snapshot -n local $output

# Push.
run tg push $id

# Remove the remote.
run tg remote delete default

# Clean and confirm the object no longer exists.
run tg clean
let output = tg get --blobs --depth=inf --pretty $id | complete
failure $output "should fail to get the object after clean"

# Confirm the object exists on the remote.
let output = run tg -u $remote.url object get --blobs --depth=inf --pretty $id
snapshot -n remote $output

# Add the remote back.
run tg remote put default $remote.url

# Check out the artifact, confirming it is pulled from the remote.
let path = $tmp | path join "output"
run tg checkout $id $path
snapshot -n checkout --path $path
