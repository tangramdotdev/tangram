use ../lib/test.nu *

# A routed run schedules its process before the command push finishes, and the runner uses the
# command's sync token to read its graph while it is still in flight.

let root_token = random chars

# Spawn a scheduler with checkpoints enabled and no local runner.
let store = { object_max_batch: 1 }
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { control: { index_timeout: 60 }, get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn a separate runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
	sync: { control: { index_timeout: 60 } },
}

# Create a user and a local server that routes runs through the scheduler.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
	sync: { control: { index_timeout: 60 } },
}

# Store an executable command only on the local server.
let architecture = (^uname -m | str trim | str replace arm64 aarch64)
let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
let host = $"($architecture)-($operating_system)"
let contents = "#!/bin/sh\necho hello\n"
let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
let blob = tg --url $local.url put $blob_value | str trim
let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
let file = tg --url $local.url put $file_value | str trim
let value = (
	['tg.command({"executable":{"artifact":' $file '},"host":' ($host | to json) '})']
	| str join
)
let command = (
	tg --url $local.url put $value
	| str trim
)

# Hold the executable blob so the scheduler receives the command before its full graph.
let store_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw)
	| from json
	| get watch
)
let retain_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.control.request.retain --params ({ node: $blob } | to json --raw)
	| from json
	| get watch
)
let state_watch = (
	tg --url $runner.url checkpoint watch runner.process.state.inserted
	| from json
	| get watch
)

let run = job spawn {
	let job_id = job id
	let output = tg --url $local.url run --cached=false --no-tty --remote --user $alice.user.id $command | complete
	$output | job send --tag $job_id 0
}

# The command transfer reaches the scheduler but remains blocked before the executable is complete.
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store_watch 0 | complete
success $output "the command push should reach the executable blob"

# The scheduler assigns the process while the command transfer is still blocked.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete
success $output "the process should be scheduled before the command push finishes"
tg --url $runner.url checkpoint continue runner.process.state.inserted $state_watch 0
tg --url $runner.url checkpoint unwatch runner.process.state.inserted $state_watch

# The runner uses the transient command sync token to request the executable from the in-flight push.
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.control.request.retain $retain_watch 0 | complete
success $output "the runner should request the in-flight executable with its sync token"
tg --url $remote.url --token $root_token checkpoint continue sync.control.request.retain $retain_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.request.retain $retain_watch

# The process cannot finish until the held command graph is stored.
let output = try { job recv --tag $run --timeout 1sec } catch { null }
assert equal $output null "the run should wait for the held command"

# Release the command transfer and verify that the process completes.
tg --url $remote.url --token $root_token checkpoint continue sync.get.store.object $store_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store_watch
let output = job recv --tag $run --timeout 30sec
success $output "the run should complete after the command push finishes"
assert ($output.stdout | str contains 'hello') "the process should produce its output"
