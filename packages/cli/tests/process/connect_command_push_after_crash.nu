use ../lib/test.nu *

# A connect's command push whose connection breaks when the remote crashes retries and completes.
let root_token = random chars

# Spawn a scheduler with checkpoints enabled and no local runner.
let store = { object_max_batch: 1 }
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn a separate runner.
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}

# Create a user and a local server that routes runs through the scheduler.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Store an executable command only on the local server.
let architecture = (^uname -m | str trim | str replace arm64 aarch64)
let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
let host = $"($architecture)-($operating_system)"
let contents = "#!/bin/sh\necho hello\n"
let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
let blob = tg --url $local.url put --no-tokens $blob_value | referent node
let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
let file = tg --url $local.url put --no-tokens $file_value | referent node
let value = (
	['tg.command({"executable":{"artifact":' $file '},"host":' ($host | to json) '})']
	| str join
)
let command = tg --url $local.url put --no-tokens $value | referent node | str trim

# Hold the executable blob so that the command push is in progress when the remote restarts.
let store_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw)
	| from json
	| get watch
)
let push_watch = tg --url $local.url checkpoint watch process.connect.command.push.finished | from json | get watch
let run = job spawn {
	let job_id = job id
	let output = tg --url $local.url process spawn --no-tokens --cached=false --sandbox --no-tty --remote --user $alice.user.id $command | complete
	$output | job send --tag $job_id 0
}
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store_watch 0 | complete) "the command push should reach the executable blob"
let output = job recv --tag $run --timeout 30sec
success $output "spawn should return while the command push is blocked"

# Crash and restart the remote without removing its runners, so that the command push's connection breaks partway through its transfer.
let pid = open --raw ($remote.directory | path join lock) | str trim | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}
let remote = server start $remote

# The retried push must complete.
let finished = timeout 60s tg --url $local.url checkpoint wait process.connect.command.push.finished $push_watch 0 | complete
tg --url $local.url checkpoint unwatch process.connect.command.push.finished $push_watch
success $finished "the command push should complete after the remote crashes"
