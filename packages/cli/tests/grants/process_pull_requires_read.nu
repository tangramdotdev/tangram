use ../../test.nu *

# Pulling a process must require read access: a principal that cannot read a process on a remote must not be able to pull it onto her own server. The remote's sync send path must authorize the caller before shipping the process.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds a private process and pushes it to the remote.
let path = artifact { tangram.ts: 'export default function () { return 5; }' }
let process = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $process
tg --url $alice_local.url index
tg --url $alice_local.url push $process
tg --url $remote.url index

# Eve has her own server that talks to the remote as Eve.
let eve_local = spawn --name eve-local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}

# Eve cannot read Alice's private process directly; it is masked as not found.
let denied = tg --url $remote.url --token $eve.token get $process | complete
failure $denied "Eve should not read Alice's private process."

# Eve must not be able to pull the private process from the remote either.
let pulled = tg --url $eve_local.url pull $process | complete
failure $pulled "Eve must not pull a process she cannot read."
