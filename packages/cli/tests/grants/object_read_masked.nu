use ../../test.nu *

# A private object is masked as not found until its owner grants the reader the subtree.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
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

# Eve cannot read Alice's private file; it is masked as not found rather than reported as unauthorized, so its existence is not revealed.
let denied = tg --url $remote.url --token $eve.token get $file | complete
failure $denied "Eve should not read Alice's private file."
snapshot ($denied.stderr | redact | normalize_ids) '
	error an error occurred
	-> failed to load the object

'

# After Alice grants Eve the file subtree, Eve can read it.
tg --url $remote.url --token $alice.token grant $eve.user.id object_subtree $file | ignore
let output = tg --url $remote.url --token $eve.token get $file | complete
success $output "Eve should read the file after Alice grants the subtree."
