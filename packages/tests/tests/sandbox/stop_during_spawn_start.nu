use ../../util/test.nu *

# A process that the sandbox's runner accepted and is starting when the sandbox stops finishes exactly once, and its spawn does not fail.

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

let path = artifact {
	tangram.ts: '
		export function hello() {
			return tg.command({ args: ["-ec", "echo hello"], executable: "/bin/sh", host: tg.host.current });
		}
	',
}
let id = checkin push $remote $path --token $root_token
let command = tg --url $url --token $root_token build $'($id)#hello' | str trim
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens | referent node

# Spawn into the sandbox, and hold the runner after it accepts the process and before the process connects.
let connect = checkpoint watch runner.process.control.connect --url $runner.url
let job = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token process spawn --no-tokens $'--sandbox=($sandbox)' $command | complete | job send --tag $id 0
}
let hit = checkpoint wait $connect
let process = $hit.params.process

# Stop the sandbox while the process is starting, which returns once the runner accepts the stop, and let the process continue.
success (timeout 30s tg --url $url --token $root_token sandbox stop $sandbox | complete) "the stop should succeed"
checkpoint unwatch $connect

# The spawn returns the process, which finishes once.
let output = job recv --tag $job --timeout 60sec
success $output "the spawn should return the process"
assert equal ($output.stdout | referent node) $process "the spawn should return the process that the runner accepted"
let outcome = tg --url $url --token $root_token wait $process | from json
let data = tg --url $url --token $root_token process get $process | from json
assert equal $data.status finished "the process should be finished"
assert equal $data.exit $outcome.exit "the process should finish once with one exit"
success (timeout 30s tg --url $url --token $root_token sandbox status --until stopped $sandbox | complete) "the sandbox should stop"
