use ../../util/test.nu *

# Pushing a referent to one remote gets its object and the object's descendants from another remote with the referent's token for that remote, under both eager and lazy push.

def test [...args] {
	# Spawn remote A with authentication and remote B.
	let a = server spawn --cloud --name a --config { authentication: { users: { providers: { insecure: true } } } }
	let b = server spawn --cloud --name b

	# Alice stores a private directory on remote A.
	let alice = tg --url $a.url login --verbose --name alice | from json
	let artifact = '
		tg.directory({
			"hello.txt": tg.file("Hello, World!"),
			"nested": tg.directory({
				"goodbye.txt": tg.file("Goodbye, World!"),
			}),
		})
	'
	let output = tg --url $a.url --token $alice.token put $artifact
	let id = $output | referent node
	let token = $output | referent tokens local | first

	# Spawn a local server that uses remote A as Bob.
	let bob = tg --url $a.url login --verbose --name bob | from json
	let local = server spawn --name local --config {
		remotes: {
			a: { token: $bob.token, url: $a.url },
			b: { url: $b.url },
		}
	}

	# Confirm that pushing the directory without the token fails.
	let output = tg --url $local.url push $id --remote=b ...$args | complete
	failure $output "should fail to push the directory without the token"

	# Push the directory to remote B with Alice's token for remote A.
	let referent = $'($id)?tokens[remote:a][0]=($token | url encode --all)'
	let output = tg --url $local.url push $referent --remote=b ...$args | complete
	success $output "should push the directory to remote B"

	# Confirm the directory on remote B matches the directory on remote A.
	let a_object = tg --url $a.url --token $alice.token object get --blobs --depth=inf --no-tokens --pretty $id
	let b_object = tg --url $b.url object get --blobs --depth=inf --no-tokens --pretty $id
	assert equal $b_object $a_object
}

test "--eager"
test "--lazy"
