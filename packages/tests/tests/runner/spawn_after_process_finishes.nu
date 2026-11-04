use ../../util/test.nu *

# A spawn must succeed when its process finishes and its sandbox ends before the spawn observes that the process started.
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
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

# Hold the spawn after the sandbox connects, and let the process finish.
let wait = checkpoint watch process.spawn.connection.wait --url $remote.url --token $root_token
let finished = checkpoint watch runner.process.control.finished --url $runner.url
let path = artifact { tangram.ts: 'export default () => 42' }
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}
checkpoint wait $wait | ignore
checkpoint pass $finished | ignore
checkpoint unwatch $finished

# Let the spawn continue after its sandbox has ended.
sleep 500ms
checkpoint continue $wait
checkpoint unwatch $wait

let output = job recv --tag $build --timeout 30sec
success $output "the build must succeed when its process finishes before the spawn observes it"
assert equal ($output.stdout | str trim) "42"
