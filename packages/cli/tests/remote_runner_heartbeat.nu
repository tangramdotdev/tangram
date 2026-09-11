use ../test.nu *

let root_token = random chars
let config = {
	advanced: {
		single_process: false,
	},
	authentication: { root: { token: $root_token } },
	indexer: { log_compaction: false },
	roles: [api indexer scheduler],
	scheduler: {
		runner_ttl: 3,
	},
}
let remote = server spawn --name remote --cloud --preserve-keys --config $config
let created = tg --url $remote.url --token $root_token runner create | from json

let runner = server spawn --name runner --config {
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

let process = tg --url $local.url run --detach $path --remote
let empty_process = tg --url $local.url run --detach $"($path)#empty" --remote
wait_until {
	let output = tg --url $remote.url --token $root_token log --timeout 0 $process | complete
	$output.stdout == "stdout before expiration\n" and $output.stderr == "stderr before expiration\n"
}
wait_until { (tg --url $remote.url --token $root_token process status $empty_process | from json | first) == "started" }

# Simulate a process that finished before its writer could send the log end request.
let data = tg --url $remote.url --token $root_token get $empty_process | from json
let data = $data | upsert children [] | upsert exit 0 | upsert finished_at $data.started_at | upsert status finished
$data | to json | tg --url $remote.url --token $root_token process put $empty_process

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid

# Wait for the runner to stop.
if $nu.os-info.name == "linux" { ^tail --pid $pid -f /dev/null } else { while (ps | where pid == $pid | is-not-empty) { sleep 10ms } }

let output = tg --url $local.url process wait $process | complete
snapshot $output.stdout '
	{"error":{"code":"heartbeat_expiration","message":"heartbeat expired"},"exit":1}

'
snapshot $output.stderr ''

tg --url $local.url process wait $empty_process | ignore

# The stored markers close nonempty and empty logs without compaction, including after a restart.
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
