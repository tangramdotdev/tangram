use ../../util/test.nu *

# The process that a new sandbox is created with finishes when the sandbox stops before the process connects.

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
		export function sleep() {
			return tg.command({ args: ["-ec", "sleep 3600"], executable: "/bin/sh", host: tg.host.current });
		}
	',
}
let id = checkin push $remote $path --token $root_token
let command = tg --url $url --token $root_token build $'($id)#sleep' | str trim

# Spawn the process with a new sandbox, and hold the runner before the process connects.
let connect = checkpoint watch runner.process.control.connect --url $runner.url
let job = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token process spawn --no-tokens $command | complete | job send --tag $id 0
}
let hit = checkpoint wait $connect
let process = $hit.params.process
let output = job recv --tag $job --timeout 60sec
success $output "the spawn should return the process"
let sandbox = tg --url $url --token $root_token process get $process | from json | get sandbox

# Stop the sandbox while the process has not connected, and let the process continue.
success (timeout 30s tg --url $url --token $root_token sandbox stop $sandbox | complete) "the stop should succeed"
checkpoint unwatch $connect

# The process finishes, and the sandbox stops.
success (timeout 60s tg --url $url --token $root_token process status --until finished $process | complete) "the process should finish"
success (timeout 30s tg --url $url --token $root_token sandbox status --until stopped $sandbox | complete) "the sandbox should stop"
