use ../../util/test.nu *

# A wait that locates a process by an attach's nonce before the attach exists receives the process's outcome, and a wait by a nonce that no attach owns is not found.

const script = path self ../../util/process_nonce.mjs
let server = server spawn --config {
	advanced: { checkpoints: true },
	process: { attach: { service: { request_timeout: 5 } } },
}
let socket = $server.url | str replace 'http+unix://' '' | url decode

# Store an executable command.
let architecture = (^uname -m | str trim | str replace arm64 aarch64)
let operating_system = if $nu.os-info.name == 'macos' { 'darwin' } else { $nu.os-info.name }
let host = $"($architecture)-($operating_system)"
let contents = "#!/bin/sh\nexit 0\n"
let blob_value = ['tg.blob(' ($contents | to json) ')'] | str join
let blob = tg put --no-tokens $blob_value | referent node
let file_value = ['tg.file({"contents":' $blob ',"executable":true})'] | str join
let file = tg put --no-tokens $file_value | referent node
let value = ['tg.command({"executable":{"artifact":' $file '},"host":' ($host | to json) '})'] | str join
let command = tg put --no-tokens $value | referent node | str trim

# Wait by the nonce, and hold the attach until the wait has requested the nonce's process.
let nonce = node $script nonce | str trim
let watch = tg checkpoint watch process.attach.service.heartbeat.request | from json | get watch
let wait = job spawn {
	let job_id = job id
	node $script wait $socket $nonce | complete | job send --tag $job_id 0
}
success (tg checkpoint wait --timeout 30s process.attach.service.heartbeat.request $watch 0 | complete) "the wait should look for the nonce's owner"
tg checkpoint unwatch process.attach.service.heartbeat.request $watch

# Spawn the process through an attach with the nonce.
let attached = node $script attach $socket $command $nonce | complete
success $attached "the attach should spawn the process"
assert equal ($attached.stdout | from json | get exit) 0

# The wait receives the outcome.
let waited = job recv --tag $wait --timeout 30sec
success $waited "the wait should complete"
assert equal ($waited.stdout | from json | get exit) 0

# A wait by a nonce that no attach owns is not found.
let unknown = node $script wait $socket (node $script nonce | str trim) | complete
success $unknown "the wait should complete"
assert equal ($unknown.stdout | from json | get status) 404
