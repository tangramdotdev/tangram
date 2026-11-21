use ../../util/test.nu *

# A stop of a sandbox that arrives while its runner has accepted a process and not yet started it ends the process failed, and the sandbox stops.

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
			return tg.command({
				args: ["-ec", "sleep 3600"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let id = checkin push $remote $path --token $root_token
let command = tg --url $url --token $root_token build $'($id)#sleep' | str trim
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens | referent node

# Spawn into the sandbox, and hold the runner once it has accepted the process.
let accepted = checkpoint watch runner.process.accepted --params { sandbox: $sandbox } --url $runner.url
let process = tg --url $url --token $root_token process spawn --no-tokens --verbose $'--sandbox=($sandbox)' $command | from json | get process
checkpoint wait $accepted | ignore

# Stop the sandbox, and let the runner continue.
let stop = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token sandbox stop --wait $sandbox | complete | job send --tag $id 0
}
checkpoint unwatch $accepted
success (job recv --tag $stop --timeout 60sec) "the stop should succeed"

# The process ends failed, and the sandbox is stopped.
let outcome = timeout 60s tg --url $url --token $root_token wait $process | from json
assert ($outcome.exit != 0) "the process should fail"
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped"
