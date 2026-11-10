use ../../util/test.nu *

# After pushing an object to one remote and cleaning it locally, pushing the object to another remote gets it from the first remote.

# Spawn two remotes and a local server.
let a = server spawn --cloud --name a
let b = server spawn --cloud --name b
let local = server spawn --name local --config {
	remotes: {
		a: { url: $a.url },
		b: { url: $b.url },
	}
}

# Create the artifact.
let artifact = '
	tg.directory({
		"hello.txt": tg.file("Hello, World!"),
		"nested": tg.directory({
			"goodbye.txt": tg.file("Goodbye, World!"),
		}),
	})
'
let id = tg put --no-tokens $artifact | referent node
let local_object = tg object get --blobs --depth=inf --no-tokens --pretty $id

# Push the object to remote A.
tg push $id --remote=a

# Clean and confirm the object no longer exists locally.
tg clean
let output = tg object get --local --no-tokens $id | complete
failure $output "should fail to get the object locally after clean"

# Push the object to remote B.
let output = tg push $id --remote=b | complete
success $output "should push the object to remote B"

# Confirm the object on remote B matches the original.
let b_object = tg --url $b.url object get --blobs --depth=inf --no-tokens --pretty $id
assert equal $b_object $local_object
