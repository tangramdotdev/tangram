use ../../util/test.nu *

# Deleting a runner while a start of its stopped sandbox has reserved the sandbox's next boot leaves the reserved boot to the scheduler, so that the deletion neither ends the boot that the start reserved nor destroys the sandbox.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

# Create a sandbox and stop it.
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
tg --url $remote.url --token $root_token sandbox stop --wait $sandbox

# Start the sandbox, and hold the start once its reservation of the next boot applies.
let reserved = checkpoint watch scheduler.sandbox.reserved --params { sandbox: $sandbox } --token $root_token --url $remote.url
let url = $remote.url
let start = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token sandbox start $sandbox | complete | job send --tag $id 0
}
checkpoint wait $reserved | ignore

# Delete the runner while the scheduler holds the reserved boot.
success (timeout 60s tg --url $remote.url --token $root_token runner delete $created.data.id | complete) "the runner should be deleted"

# The reserved boot was neither ended nor destroyed.
let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.boot 1 "the sandbox should be at the reserved boot"
assert ($data.status != 'destroyed') "the deletion should not destroy the reserved boot"
assert ($data.status != 'stopped') "the deletion should not end the reserved boot"

# Let the start continue.
checkpoint unwatch $reserved
job recv --tag $start --timeout 60sec | ignore
