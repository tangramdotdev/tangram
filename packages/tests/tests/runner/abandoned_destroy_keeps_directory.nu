use ../../util/test.nu *

# A runner whose scheduler declares it dead while the runner destroys a sandbox abandons the boot and keeps the sandbox's directory, because the scheduler's end leaves the sandbox stopped on the runner, so that the sandbox starts again with its files.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", scheduler_ttl: 1, token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'
let url = $remote.url

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
let write = tg --url $url --token $root_token build $'($id)#write' | str trim
let read = tg --url $url --token $root_token build $'($id)#read' | str trim

# Create a sandbox with a nonzero ttl, and write a file in it.
let sandbox = tg --url $url --token $root_token sandbox create --wait --ttl 1h --no-tokens | referent node
success (tg --url $url --token $root_token run $'--sandbox=($sandbox)' $write | complete) "the write should succeed"

# Destroy the sandbox, and hold the runner's teardown once the sandbox has shut down.
let shutdown = checkpoint watch runner.sandbox.shutdown --params { sandbox: $sandbox } --url $runner.url
success (tg --url $url --token $root_token sandbox destroy $sandbox | complete) "the destroy should be accepted"
checkpoint wait $shutdown | ignore

# Hold the runner's heartbeats, and advance the remote's clock past the runner ttl, so that the scheduler declares the runner dead and ends the boot during the destroy, which leaves the sandbox stopped on the runner.
let heartbeat = checkpoint watch runner.control.heartbeat.received --params { runner: $created.data.id } --token $root_token --url $url
checkpoint wait $heartbeat 0 | ignore
let expired = checkpoint watch scheduler.runner.expired --params { runner: $created.data.id } --token $root_token --url $url
let abandoned = checkpoint watch runner.boot.abandoned --params { sandbox: $sandbox } --url $runner.url
advance_time $remote 4sec
checkpoint pass $expired | ignore
checkpoint unwatch $expired
tg --url $url --token $root_token index
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the scheduler should stop the sandbox"

# Let the runner's teardown continue once the runner abandoned the boot, which it learns from the index's record of the end.
checkpoint pass $abandoned | ignore
checkpoint unwatch $abandoned
checkpoint unwatch $shutdown

# Let the runner's heartbeats continue, and advance the remote's clock past the runner's scheduler ttl, so that the runner connects again.
let connect = checkpoint watch scheduler.runner.connect --params { runner: $created.data.id } --token $root_token --url $url
checkpoint unwatch $heartbeat
advance_time $remote 2sec
checkpoint pass $connect | ignore
checkpoint unwatch $connect

# The sandbox starts again on the runner with its files.
success (timeout 60s tg --url $url --token $root_token sandbox start --wait $sandbox | complete) "the sandbox should start"
let output = tg --url $url --token $root_token run $'--sandbox=($sandbox)' $read | complete
success $output "the read should succeed"
assert ($output.stdout | str contains "hello") "the file should survive the destroy that the scheduler's end replaced"
