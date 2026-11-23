use ../../util/test.nu *

# A runner whose scheduler declares it dead while a boot is still starting on it, and that reconnects after the sandbox was started again, gives up the first boot without starting its process, even though the scheduler offers it the sandbox's next boot on the same connection, and runs the next boot only after the first boot is done with the sandbox's directory.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires the sandbox pool'
}
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
	runner: { id: $created.data.id, remote: "default", sandbox_pool_size: 1, scheduler_ttl: 1, token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'

# Create a directory to mount, to which each run of the command appends a line.
let directory = mktemp --directory
let log = $directory | path join log
let module = '
	export function append() {
		return tg.command({
			args: ["-ec", "echo ran >> \"$0\"", LOG],
			executable: "/bin/sh",
			host: tg.host.current,
		});
	}
' | str replace 'LOG' ($log | to json --raw)
let path = artifact { tangram.ts: $module }
let id = checkin push $remote $path --token $root_token
let command = tg --url $remote.url --token $root_token build $'($id)#append' | str trim

# Spawn the command in a new sandbox with a nonzero ttl, and hold the boot's task on the runner before it stores the boot's state, once the scheduler has handed the boot off to the runner and the runner has connected the boot's control stream.
let take = checkpoint watch runner.sandbox.pool.take --url $runner.url
let handoff = checkpoint watch scheduler.sandbox.handoff --token $root_token --url $remote.url
let connected = checkpoint watch sandbox.control.index.submitted --token $root_token --url $remote.url
let process = tg --url $remote.url --token $root_token spawn --sandbox --ttl 1h --mount $'($directory):($directory)' --no-tokens --verbose $command | from json | get process
let sandbox = tg --url $remote.url --token $root_token process get $process | from json | get sandbox
checkpoint wait $take | ignore
checkpoint pass $handoff | ignore
checkpoint unwatch $handoff
checkpoint pass $connected | ignore
checkpoint unwatch $connected

# Hold the runner's heartbeats, and advance the remote's clock past the runner ttl, so that the scheduler declares the runner dead and ends the boot while the runner keeps starting it.
let heartbeat = checkpoint watch runner.control.heartbeat.received --params { runner: $created.data.id } --token $root_token --url $remote.url
checkpoint wait $heartbeat 0 | ignore
let expired = checkpoint watch scheduler.runner.expired --params { runner: $created.data.id } --token $root_token --url $remote.url
advance_time $remote 4sec
checkpoint pass $expired | ignore
checkpoint unwatch $expired
tg --url $remote.url --token $root_token index
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the scheduler should stop the sandbox when it declares the runner dead"

# Start the sandbox again while the runner is cut off, so that the scheduler holds the sandbox's next boot for the runner.
let start = job spawn {
	timeout 60s tg --url $remote.url --token $root_token sandbox start $sandbox | complete | job send 0
}

# Let a heartbeat reach the scheduler, which does not acknowledge it, and advance the remote's clock past the runner's scheduler ttl, so that the runner's control connection ends and the runner connects again, learns that the first boot ended, and accepts the offer of the next boot.
let offered = checkpoint watch runner.control.offer_boot.accepted --params { sandbox: $sandbox } --url $runner.url
checkpoint continue $heartbeat 0
checkpoint wait $heartbeat 1 | ignore
let connect = checkpoint watch scheduler.runner.connect --params { runner: $created.data.id } --token $root_token --url $remote.url
advance_time $remote 2sec
checkpoint unwatch $heartbeat
checkpoint pass $connect | ignore
checkpoint unwatch $connect
checkpoint pass $offered | ignore
checkpoint unwatch $offered

# Let the first boot's task continue.
checkpoint unwatch $take

# The start succeeds, and the first boot's process fails without running.
success (job recv --timeout 60sec) "the start should succeed"
let outcome = timeout 60s tg --url $remote.url --token $root_token wait $process | from json
assert ($outcome.exit != 0) "the process should fail"

# The command runs once in the next boot.
let output = timeout 60s tg --url $remote.url --token $root_token run --no-tokens $'--sandbox=($sandbox)' $command | complete
success $output "the run should succeed"
assert equal (open --raw $log | lines) [ran] "the command should run once"
let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status started "the sandbox should be started"
assert equal $data.boot 1 "the sandbox should be at its second boot"
