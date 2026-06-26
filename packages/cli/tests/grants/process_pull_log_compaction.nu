use ../../test.nu *

# Pulling a process with its logs must not let a node-only reader obtain a live log. When the log is not yet compacted, the sync send path compacts it on demand by writing it as a blob; that write runs as the caller and grants her the blob, so a reader holding only the process node (not the log) gains the log content. The remote's finalizer is disabled so the log stays live, reproducing the window that exists transiently between a process finishing and being finalized (and that a running process is in throughout).

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } }, process: { finalizer: false } }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

# Alice builds a process on the remote whose stdout holds a secret. With the finalizer disabled the log is never compacted, so it stays live (data.log is null).
let path = artifact { tangram.ts: 'export default function () { console.log("alicesecret"); }' }
let process = tg --url $remote.url --token $alice.token build --detach $path | str trim
wait_until { (tg --url $remote.url --token $alice.token process status $process | from json | get 0) == "finished" } --timeout 30sec

# Sanity: the log is live and Alice can read her secret.
let alice_log = tg --url $remote.url --token $alice.token process log $process | complete
snapshot $alice_log.stdout '
	alicesecret

'

# Alice grants Eve only the process subtree (node read), not the log.
tg --url $remote.url --token $alice.token grant $eve.user.id process_subtree $process | ignore

# Eve has her own server that talks to the remote as Eve.
let eve_local = spawn --name eve-local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}

# Eve pulls the process with its logs. The remote must not compact and ship the log she cannot read.
tg --url $eve_local.url pull $process --logs | complete

# Eve must not be able to read Alice's private log content on her own server; the masked log is empty.
let eve_log = tg --url $eve_local.url process log $process | complete
snapshot $eve_log.stdout ''
