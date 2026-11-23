use ../../util/test.nu *

# A runner that starts a boot whose offer's answer the scheduler never receives, and whose heartbeat then expires while it keeps running, stops the boot when it connects again, so the sandbox stays stopped and starts again with its files.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { offer_timeout: 1, runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", scheduler_ttl: 1, token: $created.token.token },
}

let path = artifact {
	tangram.ts: '
		export function write() {
			return tg.command({
				args: ["-ec", "echo hello > /tmp/file"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
		export function read() {
			return tg.command({
				args: ["-ec", "read -r line < /tmp/file; echo \"$line\""],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let id = checkin push $remote $path --token $root_token
let write = tg --url $remote.url --token $root_token build $'($id)#write' | str trim
let read = tg --url $remote.url --token $root_token build $'($id)#read' | str trim

# Create the sandbox, write a file in it, and stop it.
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
success (tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"
tg --url $remote.url --token $root_token sandbox stop --wait $sandbox

# Start the sandbox, and hold the runner after it starts the boot and before it answers the offer, which also holds its heartbeats.
let accepted = checkpoint watch runner.control.offer_boot.accepted --params { sandbox: $sandbox } --url $runner.url
let url = $remote.url
let start = job spawn {
	let job_id = job id
	timeout 30s tg --url $url --token $root_token sandbox start --wait $sandbox | complete | job send --tag $job_id 0
}
checkpoint wait $accepted | ignore

# Hold the index writes, so that the scheduler keeps the ended boot's entry, and advance the remote's clock past the runner ttl, so that the scheduler expires the runner and ends the boot as stopped.
let index = checkpoint watch index.write --token $root_token --url $remote.url
let ended = checkpoint watch scheduler.boot.end --params { sandbox: $sandbox } --token $root_token --url $remote.url
advance_time $remote 4sec
checkpoint pass $ended | ignore
checkpoint unwatch $ended

# Let the runner answer the offer, and advance the remote's clock past the runner's scheduler ttl, so that the runner's control connection ends and the runner connects again while the scheduler keeps the ended boot's entry.
let connect = checkpoint watch scheduler.runner.connect --params { runner: $created.data.id } --token $root_token --url $remote.url
checkpoint unwatch $accepted
advance_time $remote 2sec
checkpoint wait $connect | ignore
checkpoint unwatch $connect
checkpoint unwatch $index

# Wait for the first start, which succeeds or fails depending on whether it saw the boot start before the runner expired.
job recv --tag $start --timeout 30sec | ignore

# The sandbox is stopped, and starts again on its runner with its files.
tg --url $remote.url --token $root_token index
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped"
success (timeout 60s tg --url $remote.url --token $root_token sandbox start --wait $sandbox | complete) "the start should succeed"
let output = tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the expiration"
