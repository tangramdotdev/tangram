use ../lib/test.nu *

# A runner's heartbeats share one HTTP/2 connection with its process control streams. When the remote stops reading an outcome sync, the unread body exhausts the connection-level flow control window, the heartbeats cannot be sent, and the scheduler expires the runner.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	cache: { map_size: 268_435_456 },
	http: { http2_connection_window_size: 16384, http2_stream_window_size: 16384 },
	index: { map_size: 268_435_456 },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	cache: { map_size: 268_435_456 },
	index: { map_size: 268_435_456 },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	cache: { map_size: 268_435_456 },
	index: { map_size: 268_435_456 },
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The output is incompressible and far larger than the connection window and the remote's read-ahead buffers.
let path = artifact {
	tangram.ts: 'export default () => {
	const leaves = [];
	for (let n = 0; n < 512; n++) {
		const bytes = new Uint8Array(128 * 1024);
		const words = new Uint32Array(bytes.buffer);
		for (let i = 0; i < words.length; i++) {
			words[i] = Math.floor(Math.random() * 4294967296);
		}
		leaves.push(bytes);
	}
	return tg.blob(...leaves);
};',
}
let sync_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}
success (timeout 60s tg --url $runner.url checkpoint wait runner.process.outcome.sync.started $sync_watch 0 | complete) 'the runner should reach the outcome sync'

# Hold the remote's sync input so the rest of the outcome stays unread in the connection.
let expired_watch = tg --url $remote.url --token $root_token checkpoint watch scheduler.runner.expired --params ({ runner: $created.data.id } | to json --raw) | from json | get watch
let input_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.input.object | from json | get watch
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $sync_watch
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.input.object $input_watch 0 | complete) 'the remote should receive an outcome object'

# Once the remote's read-ahead buffers fill, the unread leaves exhaust the connection window, the heartbeats cannot be sent, and the scheduler expires the runner.
success (timeout 120s tg --url $remote.url --token $root_token checkpoint wait scheduler.runner.expired $expired_watch 0 | complete) 'the scheduler should expire the runner while the outcome sync is unread'
tg --url $remote.url --token $root_token checkpoint unwatch scheduler.runner.expired $expired_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.input.object $input_watch
job recv --tag $build --timeout 120sec | ignore
