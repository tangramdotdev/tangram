use ../../test.nu *

# A node grant does not confer the object's children, so the grantee still cannot rely on the subtree of a private file.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

# Alice stores a private file and blob on the remote.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
tg --url $alice_local.url push --lazy $file
tg --url $remote.url index

# Bob has the directory structure but not Alice's private file or blob.
tg --url $alice_local.url get --bytes $directory | tg --url $bob_local.url put --bytes --kind dir

# Alice grants Bob only the file node, which does not reach the blob child.
tg --url $remote.url --token $alice.token grant $bob.user.id object_node $file | ignore

# The node grant is insufficient to rely on the file subtree, so the push still fails.
let output = tg --url $bob_local.url --no-quiet push --lazy $directory | complete
failure $output "a node grant should not let Bob rely on the file subtree."
