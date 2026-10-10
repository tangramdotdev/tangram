use ../lib/test.nu *

# A stalled outcome sync must leave the runner control stream able to deliver heartbeats and the process control stream able to acknowledge Finish.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	cache: { map_size: 268_435_456 },
	http: { http2: { connection_window_size: 536_870_912, max_concurrent_streams: 8, stream_window_size: 33_554_432 } },
	process: { stdio: { limits: { bytes: 2_097_152, messages: 64 }, max_message_size: 32_768, max_reads: 2 } },
	sync: { flow: { limits: { bytes: 1_048_576, messages: 16 } } },
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
	process: { stdio: { limits: { bytes: 2_097_152, messages: 64 }, max_message_size: 32_768, max_reads: 2 } },
	sync: { flow: { limits: { bytes: 1_048_576, messages: 16 } } },
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	cache: { map_size: 268_435_456 },
	index: { map_size: 268_435_456 },
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The output is incompressible and larger than the stream window and the remote's read-ahead buffers.
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
let finish_watch = tg --url $runner.url checkpoint watch runner.process.control.finish.succeeded | from json | get watch
let finished_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.finished | from json | get watch
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

success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.succeeded $finish_watch 0 | complete) 'Finish should be acknowledged while the outcome sync is held'
tg --url $runner.url checkpoint unwatch runner.process.control.finish.succeeded $finish_watch

# Observe repeated heartbeats over several runner TTLs while the outcome sync is held.
let heartbeat_watch = tg --url $remote.url --token $root_token checkpoint watch runner.control.heartbeat.received --params ({ runner: $created.data.id } | to json --raw) | from json | get watch
for index in 0..8 {
	success (timeout 5s tg --url $remote.url --token $root_token checkpoint wait runner.control.heartbeat.received $heartbeat_watch $index | complete) 'heartbeats should continue while the sync window is full'
	tg --url $remote.url --token $root_token checkpoint continue runner.control.heartbeat.received $heartbeat_watch $index
}
tg --url $remote.url --token $root_token checkpoint unwatch runner.control.heartbeat.received $heartbeat_watch
failure (timeout 1s tg --url $remote.url --token $root_token checkpoint wait scheduler.runner.expired $expired_watch 0 | complete) 'the runner should remain alive while the outcome sync is held'
tg --url $remote.url --token $root_token checkpoint unwatch scheduler.runner.expired $expired_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.input.object $input_watch
let output = job recv --tag $build --timeout 120sec
success $output 'the build should finish once outcome sync resumes'
success (timeout 120s tg --url $runner.url checkpoint wait runner.process.outcome.sync.finished $finished_watch 0 | complete) 'outcome sync should complete after its input resumes'
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.finished $finished_watch
