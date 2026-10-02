use ../lib/test.nu *

# A cache hit removes the process from the pending sandbox queue.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: {
		default: {
			url: $remote.url,
		},
	},
	runner: {
		cpus: 1,
	},
}

let path = artifact {
	tangram.ts: '
		export async function blocker() {
			await tg.sleep(3);
		}

		export async function cached() {
			await tg.sleep(10);
			return "cached";
		}
	',
}

tg --url $remote.url build $"($path)#cached" | ignore
let blocker = tg --url $local.url build --no-tokens --detach $"($path)#blocker" | referent node
success (tg --url $local.url build $"($path)#cached" | complete) "the remote cache hit should win"
tg --url $local.url wait $blocker | ignore
let start = date now
let sandbox = tg --url $local.url sandbox create --no-tokens | referent node
assert ((date now) - $start < 5sec) "the unused local process candidate should be dequeued"
tg --url $local.url sandbox destroy $sandbox
