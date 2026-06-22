use ../../test.nu *

# An object_node grant confers the node but not its children, so a node grantee reads the directory itself while its entries stay masked.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private directory with a file and pushes the whole subtree.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
tg --url $alice_local.url push $directory
tg --url $remote.url index

# Alice grants Bob only the directory node.
tg --url $remote.url --token $alice.token grant $bob.user.id object_node $directory | ignore

# Bob can read the directory node he was granted.
let dir_read = tg --url $remote.url --token $bob.token get $directory | complete
success $dir_read "Bob should read the directory node he was granted."

# Bob cannot read the child file: a node grant does not confer the subtree.
let child_read = tg --url $remote.url --token $bob.token get $file | complete
failure $child_read "an object_node grant must not confer read on the children."
snapshot ($child_read.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to load the object

'
