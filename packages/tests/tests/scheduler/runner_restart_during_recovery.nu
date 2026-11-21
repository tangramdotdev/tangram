use ../../util/test.nu *
use ../../util/nats.nu *

# A boot that a runner reports to a restarted scheduler and then loses by restarting before the scheduler's recovery ends is decided from the index, like an unanswered offer to a runner that restarted. The runner wrote the boot, so the scheduler takes it over and leaves the sandbox stopped, and the sandbox can start again.
let nats = spawn_nats
let root = random chars
let common = {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root } },
	messenger: { kind: nats, url: $nats.url },
	scheduler: { runner_ttl: 300 },
}
let instance = instance --cloud --config $common

# Start a gateway that the runners connect through, and a scheduler.
let gateway = server spawn --instance $instance --name gateway --config { roles: [api] }
let scheduler = server spawn --instance $instance --name scheduler --config { roles: [api indexer scheduler] }

# Start the runner that takes the sandbox.
let first = tg --url $gateway.url --token $root runner create | from json
let first_runner = server spawn --name first --config {
	remotes: { default: { token: $first.token.token, url: $gateway.url } },
	runner: { id: $first.data.id, remote: default, scheduler_ttl: 1, token: $first.token.token },
}
wait_for_log $first_runner 'the runner is ready to accept work'

# Hold the scheduler's handoff write, so that the boot stays in the scheduler's saved queue, and let the runner start the sandbox.
let handoff = checkpoint watch scheduler.sandbox.handoff --token $root --url $scheduler.url
let sandbox = tg --url $gateway.url --token $root sandbox create --no-tokens | referent node
checkpoint wait $handoff | ignore
success (timeout 60s tg --url $gateway.url --token $root sandbox status $sandbox --until started | complete) "the sandbox must start"

# Start a second runner of the scheduler, whose report the restarted scheduler's recovery waits for.
let second = tg --url $gateway.url --token $root runner create | from json
let second_runner = server spawn --name second --config {
	remotes: { default: { token: $second.token.token, url: $gateway.url } },
	runner: { id: $second.data.id, remote: default, scheduler_ttl: 1, token: $second.token.token },
}
wait_for_log $second_runner 'the runner is ready to accept work'

# Hold both runners' next connections, and crash the scheduler before its handoff write.
let first_connection = checkpoint watch runner.connection.start --token $root --url $gateway.url --params { runner: $first.data.id }
let second_connection = checkpoint watch runner.connection.start --token $root --url $gateway.url --params { runner: $second.data.id }
let pid = open ($scheduler.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid "the scheduler must exit"
let scheduler = server start $scheduler

# Let the first runner report the sandbox to the restarted scheduler, while the second runner's report is held, so that the recovery continues.
let added = checkpoint watch scheduler.runner.added --token $root --url $scheduler.url --params { runner: $first.data.id }
checkpoint unwatch $first_connection
checkpoint pass $added | ignore
checkpoint unwatch $added

# Crash the first runner and start it again before the recovery ends.
let pid = open ($first_runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid "the first runner must exit"
let added = checkpoint watch scheduler.runner.added --token $root --url $scheduler.url --params { runner: $first.data.id }
let first_runner = server start $first_runner
checkpoint pass $added | ignore
checkpoint unwatch $added

# Let the second runner report, which ends the recovery.
checkpoint unwatch $second_connection

# The sandbox is stopped, and starts again.
success (timeout 60s tg --url $gateway.url --token $root sandbox status $sandbox --until stopped | complete) "the sandbox must be stopped"
assert equal (tg --url $gateway.url --token $root sandbox get $sandbox | from json | get data.status) stopped
success (timeout 60s tg --url $gateway.url --token $root sandbox start --wait $sandbox | complete) "the sandbox must start again"
assert equal (tg --url $gateway.url --token $root sandbox get $sandbox | from json | get data.status) started

stop_nats $nats
