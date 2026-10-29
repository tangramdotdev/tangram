use ../../util/test.nu *

# An attach's command push that retries after the remote restarts still completes.
let root_token = random chars

# Spawn a scheduler with checkpoints enabled and no local runner.
let store = { object_max_batch: 1 }
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
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
let push_watch = tg --url $local.url checkpoint watch process.attach.command.push.finished | from json | get watch
let run = job spawn {
	let job_id = job id
	let output = tg --url $local.url process spawn --no-tokens --cached=false --sandbox --no-tty --remote --user $alice.user.id $command | complete
	$output | job send --tag $job_id 0
}
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sync.get.store.object $store_watch 0 | complete) "the command push should reach the executable blob"
let output = job recv --tag $run --timeout 30sec
success $output "spawn should return while the command push is blocked"

# Terminate the remote, wait for the command push attempt to fail, and then kill and restart the remote, so that the command push must retry against a server that has no attach for its nonce.
let failure_watch = tg --url $local.url checkpoint watch push.attempt.failed | from json | get watch
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 15 $pid
success (tg --url $local.url checkpoint wait --timeout 30s push.attempt.failed $failure_watch 0 | complete) "the command push attempt must fail when the remote terminates"
kill --signal 9 $pid
wait_for_exit $pid "the remote must exit after it is killed"
let remote = server start $remote
tg --url $local.url checkpoint unwatch push.attempt.failed $failure_watch

# The retried push must complete.
let finished = tg --url $local.url checkpoint wait --timeout 60s process.attach.command.push.finished $push_watch 0 | complete
tg --url $local.url checkpoint unwatch process.attach.command.push.finished $push_watch
success $finished "the command push should complete after the remote restarts"
