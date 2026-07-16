use ../../test.nu *

# Pulling a process with its logs must not let a node-only reader obtain a live log. The sync send path compacts a live log on demand, granting the caller the resulting blob, so it must require the log permission first. The process is kept running so its log stays live.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

# Alice runs a long-running process on the remote whose stdout holds a secret. While it runs the log stays live (data.log is null).
let path = artifact { tangram.ts: 'export default async function () { console.log("alicesecret"); await tg.sleep(60); }' }
let process = tg --url $remote.url --token $alice.token run --network=true --detach $path | str trim
wait_until { (tg --url $remote.url --token $alice.token process log $process | complete | get stdout) =~ "alicesecret" } --timeout 30sec

# Sanity: the log is live and Alice can read her secret.
let alice_log = tg --url $remote.url --token $alice.token process log $process | complete
assert ($alice_log.stdout | str contains "alicesecret") "Alice should read her own live log."

# Alice grants Eve only the process subtree (node read), not the log.
tg --url $remote.url --token $alice.token grant $eve.user.id process_subtree $process | ignore

# Eve has her own server that talks to the remote as Eve.
let eve_local = spawn --name eve-local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}

# Eve pulls the process with its logs. The remote must not compact and ship the log she cannot read.
tg --url $eve_local.url pull $process --logs | complete

# Eve must not be able to read Alice's private live log content on her own server.
let eve_log = tg --url $eve_local.url process log $process | complete
assert (not ($eve_log.stdout | str contains "alicesecret")) "Eve must not read Alice's private live log content through the pull."
