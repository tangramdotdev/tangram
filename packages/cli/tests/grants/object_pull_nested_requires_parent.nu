use ../../test.nu *

# Pulling must honor nested object masking: a principal granted the subtree of one child file may pull that file but must not pull its parent directory or a sibling she was not granted.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private directory with two files on the remote.
let directory = tg --url $alice_local.url put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --url $alice_local.url index
let children = tg --url $alice_local.url children $directory | from json
let granted_file = $children | get 0
let sibling_file = $children | get 1
tg --url $alice_local.url push $directory
tg --url $remote.url index

# Alice grants Eve the subtree of one file only.
tg --url $remote.url --token $alice.token grant $eve.user.id object_subtree $granted_file | ignore

# Eve has her own server that talks to the remote as Eve.
let eve_local = spawn --name eve-local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}

# Eve may pull the file she was granted.
let granted = tg --url $eve_local.url pull $granted_file | complete
success $granted "Eve should pull the file she was granted."

# Eve must not pull the parent directory she was not granted.
let parent = tg --url $eve_local.url pull $directory | complete
failure $parent "Eve must not pull the parent directory through a child-file subtree grant."

# Eve must not pull the sibling file she was not granted.
let sibling = tg --url $eve_local.url pull $sibling_file | complete
failure $sibling "Eve must not pull a sibling file she was not granted."
