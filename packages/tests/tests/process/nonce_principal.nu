use ../../lib/test.nu *

# A wait that locates a process by an attach's nonce is rejected unless it is authenticated as the principal that opened the attach.

const script = path self ../../lib/process_nonce.mjs
let root = random chars
let server = server spawn --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } },
	process: { attach: { service: { request_timeout: 5 } } },
}
let socket = $server.url | str replace 'http+unix://' '' | url decode
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# Store an executable command.
let architecture = (^uname -m | str trim | str replace arm64 aarch64)
let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
let host = $"($architecture)-($operating_system)"
let contents = "#!/bin/sh\nexit 0\n"
let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
let blob = tg --token $alice.token put --no-tokens $blob_value | referent node
let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
let file = tg --token $alice.token put --no-tokens $file_value | referent node
let value = ['tg.command({"executable":{"artifact":' $file '},"host":' ($host | to json) '})'] | str join
let command = tg --token $alice.token put --no-tokens $value | referent node | str trim

# Wait as bob by alice's nonce, and hold alice's attach until the wait looks for the nonce's owner.
let nonce = node $script nonce | str trim
let watch = tg --token $root checkpoint watch process.attach.service.heartbeat.request | from json | get watch
let wait = job spawn {
	let job_id = job id
	node $script wait $socket $nonce $bob.token | complete | job send --tag $job_id 0
}
success (tg --token $root checkpoint wait --timeout 30s process.attach.service.heartbeat.request $watch 0 | complete) "the wait should look for the nonce's owner"
tg --token $root checkpoint unwatch process.attach.service.heartbeat.request $watch

# Spawn the process as alice through an attach with the nonce.
let attached = node $script attach $socket $command $nonce $alice.token | complete
success $attached "the attach should spawn the process"
assert equal ($attached.stdout | from json | get exit) 0

# Bob's wait is rejected.
let waited = job recv --tag $wait --timeout 30sec
success $waited "the wait should complete"
assert equal ($waited.stdout | from json | get status) 403
