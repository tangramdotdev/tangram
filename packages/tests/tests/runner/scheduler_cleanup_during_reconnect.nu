use ../../util/test.nu *
use ../../util/nats.nu *

# Cleanup that checked ownership before a runner reconnects must not finish its work after the newer registration is accepted.
let nats = spawn_nats --config { ping_interval: 1, ping_max: 1 }
let root = random chars
let common = {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root } },
	messenger: { kind: nats, url: $nats.url },
	scheduler: { runner_ttl: 2 },
}
let instance = instance --cloud --config $common

# Start a gateway and a scheduler, keeping the indexer running while the scheduler is frozen.
let gateway = server spawn --instance $instance --name gateway --config { roles: [api indexer] }
let scheduler = server spawn --instance $instance --name scheduler --config { roles: [api scheduler] }

# Register the runner with the scheduler.
let created = tg --url $gateway.url --token $root runner create | from json
let added = checkpoint watch scheduler.runner.added --url $scheduler.url --token $root --params { runner: $created.data.id }
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root } },
	remotes: { default: { token: $created.token.token, url: $gateway.url } },
	runner: { heartbeat_interval: 0.1, id: $created.data.id, remote: default, scheduler_ttl: 1, token: $created.token.token },
}
checkpoint pass $added | ignore
checkpoint unwatch $added

# Start a client to check in the artifact and dispatch the remote build.
let local = server spawn --name local --config {
	remotes: { default: { token: $root, url: $gateway.url } },
}

# Start a long-running process on the runner.
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(120);
		}
	'
}
let connected = checkpoint watch runner.process.index.signal --url $runner.url
let process = tg --url $local.url run --no-tokens --detach $path --remote | referent node
checkpoint pass $connected | ignore
checkpoint unwatch $connected

# Hold the reconnect before it sends a registration request, so the old scheduler can begin cleanup first.
let registering = checkpoint watch runner.registration.start --url $gateway.url --token $root --params { runner: $created.data.id }
let expired = checkpoint watch scheduler.runner.expired --url $scheduler.url --token $root --params { runner: $created.data.id }
let accepted = checkpoint watch runner.registration.accept --url $gateway.url --token $root --params { runner: $created.data.id }
let removed = checkpoint watch scheduler.runner.removed --url $scheduler.url --token $root --params { runner: $created.data.id }

# Freeze the scheduler until the runner reconnects, holding the new request before dispatch.
let scheduler_pid = open ($scheduler.directory | path join 'lock') | into int
^kill -STOP $scheduler_pid
checkpoint wait $registering | ignore

# Let the scheduler begin cleanup while it still owns the runner, holding it before any state writes.
^kill -CONT $scheduler_pid
checkpoint wait $expired | ignore

# Accept a newer attempt on the same scheduler, then resume the older cleanup, whose writes must now be rejected.
checkpoint unwatch $registering
checkpoint wait $accepted | ignore
checkpoint unwatch $expired
checkpoint pass $removed | ignore
checkpoint unwatch $removed
checkpoint unwatch $accepted

# Verify that the older cleanup did not finish the running process.
tg --url $gateway.url --token $root index
let status = tg --url $gateway.url --token $root get --source=index $process | from json | get status
assert ($status == 'started') 'a live runner process must not be destroyed by a stale scheduler registration'

stop_nats $nats
