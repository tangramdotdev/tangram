use ../../test.nu *

# A subtree grant on one file confers neither its parent directory nor its sibling files.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private directory with two files.
let directory = tg --url $alice_local.url put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --url $alice_local.url index
let children = tg --url $alice_local.url children $directory | from json
let granted_file = $children | get 0
let sibling_file = $children | get 1
tg --url $alice_local.url push --lazy $directory
tg --url $remote.url index

# Alice grants Eve the subtree of one file only.
tg --url $remote.url --token $alice.token grant $eve.user.id object_subtree $granted_file | ignore

# Eve can read the granted file.
let granted = tg --url $remote.url --token $eve.token get $granted_file | complete
success $granted "Eve should read the file she was granted."

# Eve cannot climb to the parent directory.
let parent = tg --url $remote.url --token $eve.token get $directory | complete
failure $parent "the grant should not confer the parent directory."

# Eve cannot reach the sibling file.
let sibling = tg --url $remote.url --token $eve.token get $sibling_file | complete
failure $sibling "the grant should not confer a sibling file."
