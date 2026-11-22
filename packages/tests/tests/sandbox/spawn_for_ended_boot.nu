use ../../util/test.nu *

# A process that the sandbox's scheduler offers to the runner of a boot that ended before the offer arrived is refused by the runner, and the scheduler finishes it as failed, so that it does not run in the sandbox's next boot.

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

# Spawn into the sandbox, and hold the scheduler before it sends the spawn to the sandbox's runner.
let spawn = checkpoint watch scheduler.process.spawn --params { sandbox: $sandbox } --token $root_token --url $url
let job = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token process spawn --no-tokens $'--sandbox=($sandbox)' $command | complete | job send --tag $id 0
}
checkpoint wait $spawn | ignore
tg --url $url --token $root_token index
let queued = tg --url $url --token $root_token ps --status queued | from json
assert equal ($queued | length) 1 "the process should be queued"
let process = $queued | first | get node

# Stop the sandbox and start its next boot.
success (timeout 30s tg --url $url --token $root_token sandbox stop --wait $sandbox | complete) "the stop should succeed"
success (timeout 30s tg --url $url --token $root_token sandbox start --wait $sandbox | complete) "the start should succeed"

# Let the scheduler offer the process, which is for the boot that ended. The runner says no, and the scheduler finishes the process as failed.
checkpoint unwatch $spawn
job recv --tag $job --timeout 60sec | ignore
let outcome = tg --url $url --token $root_token wait $process | from json
assert equal $outcome.exit 1 "the process should fail"
let process_data = tg --url $url --token $root_token process get $process | from json
assert equal $process_data.status finished "the process should be finished"
assert equal $process_data.error?.node?.message? "the spawn is for another boot of the sandbox" "the process should fail with the runner's no"
assert equal $process_data.started_at? null "the process should not run in the sandbox's next boot"
let data = tg --url $url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status started "the sandbox should be started"
assert equal $data.boot 1 "the sandbox should be at its next boot"
