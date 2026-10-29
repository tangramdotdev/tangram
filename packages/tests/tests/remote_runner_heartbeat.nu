use ../util/test.nu *

# A remote runner renews its heartbeat while it is connected.

let root_token = random chars
let config = {
	advanced: {
		multi_server: true,
	},
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: {
		runner_ttl: 3,
	},
}
let remote = server spawn --name remote --cloud --preserve-keys --now '2026-01-01T00:00:00Z' --config $config
let created = tg --url $remote.url --token $root_token runner create | from json

let runner = server spawn --name runner --config {
	advanced: { checkpoints: true }
	runner: {
		id: $created.data.id
		remote: "default"
		token: $created.token.token
	}
	remotes: {
		default: {
			token: $created.token.token
			url: $remote.url
		}
	}
}

let local = server spawn --name local --config {
	remotes: {
		default: {
			token: $root_token
			url: $remote.url
		}
	}
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log("stdout before expiration");
			console.error("stderr before expiration");
			await tg.sleep(60);
		}
		export async function empty() {
			await tg.sleep(60);
		}
	'
}

let connected = checkpoint watch runner.process.index.signal --url $runner.url
let process = tg --url $local.url run --no-tokens --detach $path --remote | referent node
let empty_process = tg --url $local.url run --no-tokens --detach $"($path)#empty" --remote | referent node

# Wait for the runner to connect each process's control and index it, and then for the remote to index the starts.
for hit in [0 1] {
	checkpoint pass $connected $hit | ignore
}
checkpoint unwatch $connected
tg --url $remote.url --token $root_token index

# Wait for the remote to receive each stream's log, reading exactly the expected length without a timeout.
for stream in [stdout stderr] {
	let expected = $"($stream) before expiration\n"
	let length = $expected | str length --utf-8-bytes
	let output = timeout 10 tg --url $remote.url --token $root_token log --no-timeout --stream $stream --length $length $process | complete
	success $output
	assert equal ($output | get $stream) $expected
}

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid

# Wait for the runner to stop, and advance the remote's clock past the runner ttl so that the scheduler expires the runner.
wait_for_exit $pid
advance_time $remote 4sec

let output = tg --url $local.url process wait $process | complete
snapshot $output.stdout '
	{"error":{"code":"internal","message":"heartbeat expired"},"exit":1}

'
snapshot $output.stderr ''

tg --url $local.url process wait $empty_process | ignore

# The process history remains readable after the runner is lost.
let sandbox = tg --url $remote.url --token $root_token get --source=index $process | from json | get sandbox
tg --url $remote.url --token $root_token sandbox wait --source=index $sandbox | ignore

# Recovery finishes nonempty and empty log objects before publishing the failed processes.
for restart in [false true] {
	let remote = if $restart { server restart $remote } else { $remote }
	let output = timeout 10 tg --url $remote.url --token $root_token log --no-timeout $process | complete
	success $output
	assert equal $output.stdout "stdout before expiration\n"
	assert equal $output.stderr "stderr before expiration\n"
	for stream in [stdout stderr] {
		let output = timeout 10 tg --url $remote.url --token $root_token log --no-timeout --stream $stream $process | complete
		success $output
		assert equal ($output | get $stream) $"($stream) before expiration\n"
	}
	let output = timeout 10 tg --url $remote.url --token $root_token log --no-timeout $empty_process | complete
	success $output
	assert equal $output.stdout ""
	assert equal $output.stderr ""
}
