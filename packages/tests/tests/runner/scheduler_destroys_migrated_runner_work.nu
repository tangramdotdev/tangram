use ../../util/test.nu *
use ../../util/nats.nu *

# A scheduler that still holds a stale registration for a runner that has migrated to another scheduler must
# not destroy the runner's still-running work when its stale registration expires.
#
# The scenario is driven entirely with checkpoints. A dedicated NATS lets the frozen scheduler's share of the
# shared queue group be dropped, so that the runner's re-registration reaches the second scheduler, leaving the
# first scheduler holding a stale registration.
let nats = spawn_nats
let root = random chars
let common = {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root } },
	messenger: { kind: nats, url: $nats.url },
	scheduler: { runner_ttl: 2 },
}
let instance = instance --cloud --config $common

# Start an api gateway and the first scheduler. The gateway bridges the runner's control connection and stays
# alive throughout, so the runner always has an entry point even while the first scheduler is frozen.
let gateway = server spawn --instance $instance --name gateway --config { roles: [api indexer] }
let scheduler_a = server spawn --instance $instance --name scheduler-a --config { roles: [api indexer scheduler] }

# Register the runner. With only the first scheduler running, it registers there deterministically.
let created = tg --url $gateway.url --token $root runner create | from json
let added_a = checkpoint watch scheduler.runner.added --url $scheduler_a.url --token $root --params { runner: $created.data.id }
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root } },
	remotes: { default: { token: $created.token.token, url: $gateway.url } },
	runner: { heartbeat_interval: 0.1, id: $created.data.id, remote: default, scheduler_ttl: 1, token: $created.token.token },
}
checkpoint pass $added_a | ignore
checkpoint unwatch $added_a

# Start a standalone client that checks in the artifact and dispatches it to the instance, since the multi-server
# instance servers cannot check in a local path.
let local = server spawn --name local --config {
	remotes: { default: { token: $root, url: $gateway.url } },
}

# Start a long-running process on the runner, tracked by the first scheduler.
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

# Start the second scheduler, which the runner can migrate to.
let scheduler_b = server spawn --instance $instance --name scheduler-b --config { roles: [api indexer scheduler] }

# Register the checkpoints on both schedulers while they are alive: the migration on the second scheduler and
# the stale registration's expiry on the first, which the first scheduler reaches only after it is thawed.
let added_b = checkpoint watch scheduler.runner.added --url $scheduler_b.url --token $root --params { runner: $created.data.id }
let expired = checkpoint watch scheduler.runner.expired --url $scheduler_a.url --token $root --params { runner: $created.data.id }

# Freeze the first scheduler so that NATS evicts it from the shared queue group while its in-memory registration
# stays alive. The runner stops receiving heartbeat acknowledgements and re-registers with the second scheduler.
let scheduler_a_pid = open ($scheduler_a.directory | path join 'lock') | into int
^kill -STOP $scheduler_a_pid
checkpoint pass $added_b | ignore
checkpoint unwatch $added_b

# Thaw the first scheduler, which still holds its now-stale registration. Its cleaner expires the stale
# registration and runs the expired-runner handler.
^kill -CONT $scheduler_a_pid
checkpoint pass $expired | ignore
checkpoint unwatch $expired

# Finish indexing on the second scheduler, then read the process. The process sleeps and can only be finished by
# the stale registration's expiry, so it must still be started.
tg --url $scheduler_b.url --token $root index
let status = tg --url $scheduler_b.url --token $root get --source=index $process | from json | get status
assert ($status == 'started') 'a live runner process must not be destroyed by a stale scheduler registration'

stop_nats $nats
