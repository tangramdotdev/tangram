use ../../util/test.nu *

# A cache hit removes the process from the pending sandbox queue.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
	remotes: {
		default: {
			url: $remote.url,
		},
	},
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
	runner: {
		cpus: 1,
		overcommit: { cpu: 1 },
	},
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(60);
		}

		export function cached() {
			return "cached";
		}
	',
}

# Cache the process at the remote.
tg --url $remote.url build $"($path)#cached" | ignore

# Start a process that holds the local runner's only capacity. The watch holds every later process start, so a local process candidate that is not dequeued keeps the capacity.
let watch = tg --url $local.url checkpoint watch runner.process.start | from json | get watch
let blocker = tg --url $local.url build --no-tokens --detach --verbose $"($path)#blocker" | from json
tg --url $local.url checkpoint wait runner.process.start $watch 0 | ignore
tg --url $local.url checkpoint continue runner.process.start $watch 0

# Build the cached process while its local candidate waits for capacity.
success (tg --url $local.url build $"($path)#cached" | complete) "the remote cache hit should win"

# Release the capacity. A sandbox gets the capacity only if the local candidate was dequeued.
tg --url $local.url cancel $blocker.process $blocker.lease
tg --url $local.url wait $blocker.process | ignore
let output = timeout 60s tg --url $local.url sandbox create --no-tokens | complete
success $output "the unused local process candidate should be dequeued"
tg --url $local.url checkpoint unwatch runner.process.start $watch
tg --url $local.url sandbox destroy ($output.stdout | referent node)
