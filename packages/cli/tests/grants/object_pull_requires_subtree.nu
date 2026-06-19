use ../../test.nu *

# Pulling an object must require its subtree: a principal without an object's subtree on a remote must not be able to pull it onto her own server. The remote's sync send path must authorize the caller before shipping bytes.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private file on the remote.
let file = tg --url $alice_local.url put 'tg.file("topsecret")' | str trim
tg --url $alice_local.url index
tg --url $alice_local.url push $file
tg --url $remote.url index

# Eve has her own server that talks to the remote as Eve.
let eve_local = spawn --name eve-local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}

# Eve does not have the subtree for Alice's private file directly; it is masked as not found.
let denied = tg --url $remote.url --token $eve.token get $file | complete
failure $denied "Eve should not have the subtree for Alice's private file."

# Eve must not be able to pull the private file from the remote either.
let pulled = tg --url $eve_local.url pull $file | complete
failure $pulled "Eve must not pull an object whose subtree she does not have."
