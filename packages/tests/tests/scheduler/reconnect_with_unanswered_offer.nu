use ../../util/test.nu *
use ../../util/nats.nu *

# A runner that reconnects while the offer of a boot to it is unanswered gets the same offer again, so that a second runner does not hold the boot and the sandbox's process runs once.
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

# Connect the first runner to the scheduler.
let first = tg --url $gateway.url --token $root runner create | from json
let added = checkpoint watch scheduler.runner.added --url $scheduler.url --token $root --params { runner: $first.data.id }
let runner = server spawn --name first --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $first.token.token, url: $gateway.url } },
	runner: { id: $first.data.id, remote: default, token: $first.token.token },
}
checkpoint pass $added | ignore
checkpoint unwatch $added
wait_for_log $runner 'the runner is ready to accept work'

# Start a client to check in the artifact and run the process on the remote.
let local = server spawn --name local --config {
	remotes: { default: { token: $root, url: $gateway.url } },
}
let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("ran");
			return 42;
		}
	'
}

# Hold the first runner before it answers the offer of the process's sandbox.
let offer = checkpoint watch runner.control.offer_boot --url $runner.url
let process = tg --url $local.url run --no-tokens --detach $path --remote | referent node
checkpoint wait $offer | ignore

# Break the first runner's control connection by crashing the gateway, and start the gateway again.
let pid = server_pid $gateway.directory
kill --signal 9 $pid
wait_for_exit $pid "the gateway must exit"
let gateway = server start $gateway

# Connect a second runner, which could take the sandbox if the scheduler offered it again.
let second = tg --url $gateway.url --token $root runner create | from json
let added = checkpoint watch scheduler.runner.added --url $scheduler.url --token $root --params { runner: $second.data.id }
let other = server spawn --name second --config {
	remotes: { default: { token: $second.token.token, url: $gateway.url } },
	runner: { id: $second.data.id, remote: default, token: $second.token.token },
}
checkpoint pass $added | ignore
checkpoint unwatch $added
wait_for_log $other 'the runner is ready to accept work'

# Let the first runner answer the offer, which it does on a broken connection, after which it reconnects.
checkpoint unwatch $offer

# The process runs once and succeeds.
let output = timeout 60s tg --url $local.url process wait $process | complete
success $output "the process must finish after the first runner reconnects"
let data = tg --url $gateway.url --token $root get $process | from json
assert equal $data.status "finished"
assert equal $data.exit 0 "the process must succeed"
let log = tg --url $local.url process log $process | complete
success $log "the process log must be readable"
let lines = $log.stdout | lines | where { |line| $line == 'ran' }
assert equal ($lines | length) 1 "the process must run once"

stop_nats $nats
