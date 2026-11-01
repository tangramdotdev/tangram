use ../../util/test.nu *

# A runner whose heartbeat expires while it keeps running a sandbox stops the sandbox when it reconnects, so the sandbox starts again with its files.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
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

let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
success (tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"

# Hold the runner's heartbeats and advance the remote's clock past the runner ttl, so that the remote expires the runner and stops the sandbox while the runner keeps running it.
let heartbeat = checkpoint watch runner.control.heartbeat.received --params { runner: $created.data.id } --token $root_token --url $remote.url
let expired = checkpoint watch scheduler.runner.expired --params { runner: $created.data.id } --token $root_token --url $remote.url
advance_time $remote 4sec
checkpoint pass $expired
checkpoint unwatch $expired
checkpoint unwatch $heartbeat
tg --url $remote.url --token $root_token index
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the remote should stop the sandbox when the runner expires"

# Kill the remote and start it again, so that the runner reconnects. The start succeeds once the reconnected runner has stopped its boot of the sandbox.
kill --signal 9 $remote.pid
wait_for_exit $remote.pid
let remote = server start $remote
success (timeout 60s tg --url $remote.url --token $root_token sandbox start $sandbox | complete) "the start should succeed after the runner reconnects"
let output = tg --url $remote.url --token $root_token run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the expiration"
