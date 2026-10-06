use lib/test.nu *

# A remote runner renews its heartbeat while it is connected.

let root_token = random chars
let config = {
	advanced: {
		single_process: false,
	},
	authentication: { root: { token: $root_token } },
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

let process = tg --url $local.url run --no-tokens --detach $path --remote | referent node
let empty_process = tg --url $local.url run --no-tokens --detach $"($path)#empty" --remote | referent node
wait_until {
	let output = tg --url $remote.url --token $root_token log --timeout 0 $process | complete
	$output.stdout == "stdout before expiration\n" and $output.stderr == "stderr before expiration\n"
}
wait_until { (tg --url $remote.url --token $root_token process status $empty_process | from json | first) == "started" }

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid

# Wait for the runner to stop.
if $nu.os-info.name == "linux" { ^tail --pid $pid -f /dev/null } else { while (ps | where pid == $pid | is-not-empty) { sleep 10ms } }

let output = tg --url $local.url process wait $process | complete
snapshot $output.stdout '
	{"error":{"code":"internal","message":"heartbeat expired"},"exit":1}

'
snapshot $output.stderr ''

tg --url $local.url process wait $empty_process | ignore

# The process history remains readable after the runner is lost.
let sandbox = tg --url $remote.url --token $root_token get --source=index $process | from json | get sandbox
tg --url $remote.url --token $root_token sandbox wait --source=index $sandbox | ignore
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let output = http get --raw --unix-socket $socket --headers { Authorization: $'Bearer ($root_token)' } $'http://localhost/sandboxes/($sandbox)/processes?source=index&timeout=0'
let processes = $output | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json | get data } | flatten
assert ($process in $processes)

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
