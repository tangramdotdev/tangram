use ../../util/test.nu *
use ../../util/nats.nu *

# A boot that a runner accepted and whose record never reached the index, because its scheduler crashed before its first write of it, is cancelled when the runner reports it to the restarted scheduler, which has the runner destroy it.
let nats = spawn_nats
let root = random chars
let common = {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root } },
	messenger: { kind: nats, url: $nats.url },
	scheduler: { runner_ttl: 300 },
}
let instance = instance --cloud --config $common

# Start a gateway that the runner connects through, a scheduler, and a runner.
let gateway = server spawn --instance $instance --name gateway --config { roles: [api] }
let scheduler = server spawn --instance $instance --name scheduler --config { roles: [api indexer scheduler] }
let created = tg --url $gateway.url --token $root runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $gateway.url } },
	runner: { id: $created.data.id, remote: default, scheduler_ttl: 1, token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'

# Hold the scheduler's writes of the sandbox and the runner's control connection of the boot, and create a sandbox, which returns once the runner accepts it.
let persist = checkpoint watch scheduler.sandbox.persist --token $root --url $scheduler.url
let handoff = checkpoint watch scheduler.sandbox.handoff --token $root --url $scheduler.url
let connect = checkpoint watch sandbox.control.connect --token $root --url $gateway.url
let sandbox = tg --url $gateway.url --token $root sandbox create --no-tokens | referent node
checkpoint wait $connect | ignore

# Hold the runner's next connection, and crash the scheduler before its writes of the sandbox.
let connection = checkpoint watch runner.connection.start --token $root --url $gateway.url --params { runner: $created.data.id }
let pid = open ($scheduler.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid "the scheduler must exit"
let scheduler = server start $scheduler

# Let the runner report the boot to the restarted scheduler, whose index does not have the boot.
let added = checkpoint watch scheduler.runner.added --token $root --url $scheduler.url --params { runner: $created.data.id }
checkpoint unwatch $connection
checkpoint pass $added | ignore
checkpoint unwatch $added

# Let the boot's control connection proceed. The runner destroys the boot.
checkpoint unwatch $connect
success (timeout 60s tg --url $gateway.url --token $root sandbox status $sandbox --until destroyed | complete) "the sandbox must be destroyed"
assert equal (tg --url $gateway.url --token $root sandbox get $sandbox | from json | get data.status) destroyed

stop_nats $nats
