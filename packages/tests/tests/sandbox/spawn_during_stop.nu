use ../../util/test.nu *

# A process that the sandbox's scheduler offers to the runner while the runner stops the sandbox is accepted into the stopping boot and finished as failed, and the sandbox stops.

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
			return tg.command({
				args: ["-ec", "echo hello"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let id = checkin push $remote $path --token $root_token
let command = tg --url $url --token $root_token build $'($id)#hello' | str trim
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens | referent node

# Stop the sandbox, and hold the runner's teardown of the boot once the sandbox has shut down.
let shutdown = checkpoint watch runner.sandbox.shutdown --params { sandbox: $sandbox } --url $runner.url
let stop = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token sandbox stop --wait $sandbox | complete | job send --tag $id 0
}
checkpoint wait $shutdown | ignore

# Spawn into the sandbox, and wait for the runner to accept the process into the stopping boot.
let accepted = checkpoint watch runner.process.accepted --params { sandbox: $sandbox } --url $runner.url
let process = tg --url $url --token $root_token process spawn --no-tokens --verbose $'--sandbox=($sandbox)' $command | from json | get process
checkpoint pass $accepted | ignore
checkpoint unwatch $accepted

# Let the teardown continue.
checkpoint unwatch $shutdown
success (job recv --tag $stop --timeout 60sec) "the stop should succeed"

# The process ends failed without running, and the sandbox is stopped.
let outcome = timeout 60s tg --url $url --token $root_token wait $process | from json
assert ($outcome.exit != 0) "the process should fail"
let data = tg --url $url --token $root_token process get $process | from json
assert equal $data.started_at? null "the process should not run"
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped"
