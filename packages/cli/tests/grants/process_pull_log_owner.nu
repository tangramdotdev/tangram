use ../../test.nu *

# The owner may still pull and read a live process log. The compaction gate that withholds the log from an unauthorized puller must not withhold it from the process owner, who is authorized to read it. The remote's finalizer is disabled so the log stays live, exercising the on-demand compaction path during the pull.

let remote = spawn --cloud --name remote --config { authentication: true, process: { finalizer: false } }

let alice = tg --url $remote.url login --verbose alice | from json

# Alice builds a process on the remote whose stdout holds a secret. With the finalizer disabled the log stays live (data.log is null).
let path = artifact { tangram.ts: 'export default function () { console.log("alicesecret"); }' }
let process = tg --url $remote.url --token $alice.token build --detach $path | str trim
wait_until { (tg --url $remote.url --token $alice.token process status $process | from json | get 0) == "finished" } --timeout 30sec

# Alice has her own server that talks to the remote as herself.
let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice pulls her own process with its logs. The on-demand compaction must run for her, since she is authorized to read her own log.
tg --url $alice_local.url pull $process --logs | complete

# Alice reads her own log on her own server.
let alice_log = tg --url $alice_local.url process log $process | complete
snapshot $alice_log.stdout '
	alicesecret

'
