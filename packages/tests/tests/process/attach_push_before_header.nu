use ../../lib/test.nu *

# A forwarding server starts its command push before the destination's attach returns its header, joining the destination's command sync by the attach's nonce.

let root_token = random chars

# Spawn a scheduler with checkpoints enabled and no local runner.
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
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
let value = ['tg.command({"executable":{"artifact":' $file '},"host":' ($host | to json) '})'] | str join
let command = tg --url $local.url put --no-tokens $value | referent node | str trim

# Hold the destination's attach before it creates its command sync.
let start_watch = tg --url $remote.url --token $root_token checkpoint watch process.attach.start | from json | get watch
let retain_watch = tg --url $remote.url --token $root_token checkpoint watch process.attach.service.retain | from json | get watch
let run = job spawn {
	let job_id = job id
	tg --url $local.url run --cached=false --no-tty --remote --user $alice.user.id $command | complete | job send --tag $job_id 0
}
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s process.attach.start $start_watch 0 | complete) "the destination's attach should start"

# The command push reaches the destination and waits for the command sync while the attach is held.
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s process.attach.service.retain $retain_watch 0 | complete) "the command push should wait for the attach's command sync"
tg --url $remote.url --token $root_token checkpoint unwatch process.attach.service.retain $retain_watch

# Release the attach, and the run completes.
tg --url $remote.url --token $root_token checkpoint unwatch process.attach.start $start_watch
let output = job recv --tag $run --timeout 30sec
success $output "the run should complete"
assert ($output.stdout | str contains 'hello') "the process should produce its output"
