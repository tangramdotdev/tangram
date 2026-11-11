use ../../util/test.nu *

# A destroy of a stopped sandbox whose server crashes after it records the destruction and before the runner removes the sandbox's directory finishes when the runner next reports its stopped sandboxes, without another destroy.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", stopped_sandbox_interval: 1, token: $created.token.token },
}

# Create a sandbox, and stop it.
let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node
tg --url $remote.url --token $root_token sandbox stop --wait $sandbox

# Destroy the sandbox, and kill the server after it records the destruction and before it asks the runner to remove the sandbox's directory.
let remove = checkpoint watch sandbox.destroy.remove --params { sandbox: $sandbox } --token $root_token --url $remote.url
tg --url $remote.url --token $root_token sandbox destroy $sandbox
checkpoint wait $remove | ignore
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid

# Start the server again. The sandbox is destroyed once the runner reports it and removes its directory.
let remote = server start $remote
mut status = ""
for _ in 0..300 {
	$status = (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status)
	if $status == destroyed { break }
	sleep 100ms
}
assert equal $status destroyed "the sandbox should be destroyed without another destroy"
