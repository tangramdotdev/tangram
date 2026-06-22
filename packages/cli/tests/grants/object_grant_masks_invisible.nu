use ../../test.nu *

# Granting an object permission the actor cannot see is masked as a missing resource, so it is not an existence oracle.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private file on the remote.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
tg --url $alice_local.url push --lazy $file
tg --url $remote.url index

# Eve cannot see Alice's private file, so granting on it reports a missing resource rather than that it exists.
let output = tg --url $remote.url --token $eve.token grant $bob.user.id object_subtree $file | complete
failure $output "Eve should not grant a permission on a file she cannot see."
snapshot ($output.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to create the grant
	   principal = <user>
	   resource = fil_010000000000000000000000000000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the resource

'
