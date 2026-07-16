use ../../test.nu *

# Granting an object permission requires only that permission, not admin, so a subtree grantee can delegate the subtree but a node grantee cannot.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json
let carol = tg --url $remote.url login --verbose carol | from json
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

# Alice grants Bob the file subtree and Eve only the file node.
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $file | ignore
tg --url $remote.url --token $alice.token grant $eve.user.id object_node $file | ignore

# Bob holds the subtree, so he can delegate it to Carol without admin.
let delegated = tg --url $remote.url --token $bob.token grant $carol.user.id object_subtree $file | complete
success $delegated "a subtree grantee should delegate the subtree."
let output = tg --url $remote.url --token $carol.token get $file | complete
success $output "Carol should read the file through Bob's delegated grant."

# Eve holds only the node, so she cannot grant the subtree she does not hold.
let escalation = tg --url $remote.url --token $eve.token grant $carol.user.id object_subtree $file | complete
failure $escalation "a node grantee should not grant the subtree."
snapshot ($escalation.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to create the grant
	   principal = <user>
	   resource = fil_010000000000000000000000000000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the resource

'
