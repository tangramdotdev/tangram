use ../../util/test.nu *

# A boot that fails right after its runner accepts it, before the scheduler receives the runner's yes and records the handoff, ends.

if $nu.os-info.name != 'linux' { skip_test 'this test requires linux mount validation' }

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let url = $remote.url

# Hold the runner after it accepts the boot and before it sends its yes.
let accepted = checkpoint watch runner.control.offer_sandbox.accepted --url $runner.url

# Create a sandbox whose creation fails on the runner.
let job = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token sandbox create --no-tokens --mount /tmp:/dev/target,ro | complete | job send --tag $id 0
}
let hit = checkpoint wait $accepted
let sandbox = $hit.params.sandbox

# The sandbox is destroyed with the creation error while the runner's yes is held.
success (timeout 30s tg --url $url --token $root_token sandbox status --until destroyed $sandbox | complete) "the sandbox should be destroyed while the yes is held"
let data = tg --url $url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status destroyed
assert equal $data.error.source.node.message "sandbox mounts at or beneath /dev, /proc, and /sys are not supported" "the sandbox should record the creation error"

# Let the runner send its yes, and the create returns the sandbox.
checkpoint unwatch $accepted
let output = job recv --tag $job --timeout 60sec
success $output "the create should return the sandbox"
