use ../../test.nu *

# A subtree grant lets the grantee rely on the granted objects already stored on the remote, transferring only the new object.

let remote = spawn --cloud --name remote --config { authentication: true }

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

# Without a grant Bob can neither transfer nor rely on Alice's private file.
let denied = tg --url $bob_local.url --no-quiet push --lazy $directory | complete
failure $denied "Bob should not rely on Alice's private file without a grant."

# Alice grants Bob the file subtree.
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $file | ignore

# Now Bob skips the granted file and blob and transfers only the directory.
let output = tg --url $bob_local.url --no-quiet push --lazy $directory | complete
success $output "Bob should rely on the granted file subtree."
assert ($output.stderr | str contains "skipped 0 processes, 2 objects") "Bob should skip the granted file and blob."
assert ($output.stderr | str contains "transferred 0 processes, 1 objects") "Bob should transfer only the directory."
