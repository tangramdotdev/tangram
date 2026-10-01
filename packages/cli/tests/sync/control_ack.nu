use ../lib/test.nu *

# Control requests retry until acknowledged, then wait for the retained request's response.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token } },
	sync: { control: { retry_interval: 0.1 } },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}
let blob = tg --url $local.url put 'tg.blob("hello")' | str trim
let params = { node: $blob } | to json --raw
let subscribe_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.subscribe | from json | get watch
let heartbeat_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.heartbeat.request | from json | get watch
let retain_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.request.retain --params $params | from json | get watch
let store_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw) | from json | get watch
let request_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.request --params $params | from json | get watch

# Hold the incoming object and the control subscription so the first heartbeats are lost.
let push_log = $env.TMPDIR | path join push.log
'' | save --force $push_log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $local.url push $blob o+e>| tee { save --force $push_log } | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store_watch 0 | ignore
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.subscribe $subscribe_watch 0 | ignore
wait_until { open --raw $push_log | str contains 'tokens[remote][0]' } 'the push should log the referent with the authorization token for the sync'
let referent = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[remote\]' } | first | str trim
let sync = $'http://localhost/($referent)' | url parse | get params | where key == 'tokens[remote][0]' | first | get value
let query = { 'tokens[local][0]': $sync } | url build-query
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let read = job spawn {
	let job_id = job id
	let headers = { Authorization: $'Bearer ($root_token)' }
	let output = http get --max-time 10sec --unix-socket $socket --headers ($headers | insert Accept 'application/json') $'http://localhost/objects/($blob)?($query)'
	$output | job send --tag $job_id 0
}

# The missing authorized read verifies node storage through the incoming sync.
# The first heartbeat also retries until the control service starts listening.
let first_heartbeat = timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.heartbeat.request $heartbeat_watch 0 | from json | get params
let retry_heartbeat_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.heartbeat.request --params ({ client: $first_heartbeat.client } | to json --raw) | from json | get watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.heartbeat.request $heartbeat_watch
let second_heartbeat = timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.heartbeat.request $retry_heartbeat_watch 0 | from json | get params
assert equal $first_heartbeat.id $second_heartbeat.id "a heartbeat retry should preserve the request ID"
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.heartbeat.request $retry_heartbeat_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.subscribe $subscribe_watch

# Node requests retain the same ID while their acknowledgement is held.
let first = timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.request $request_watch 0 | from json | get params.id
let retry_params = { node: $blob, id: $first } | to json --raw
let retry_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.request --params $retry_params | from json | get watch
let ack_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.ack --params $retry_params | from json | get watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.request $request_watch
let second = timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.request $retry_watch 0 | from json | get params.id
assert equal $first $second "a retry should preserve the request ID"
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.request $retry_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.request.retain $retain_watch
let acknowledged = timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.ack $ack_watch 0 | from json | get params.id
assert equal $acknowledged $first "the sync should acknowledge the retained request"

# No request is sent after the acknowledgement, and the acknowledgement does not complete the read.
let request_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.request --params $retry_params | from json | get watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.ack $ack_watch
let output = timeout 1s tg --url $remote.url --token $root_token checkpoint wait sync.control.request $request_watch 0 | complete
assert equal $output.exit_code 124 "the acknowledged request must not be resent"
let premature = try { job recv --tag $read --timeout 1sec } catch { null }
assert equal $premature null "the read should wait for the availability response"
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.request $request_watch

# Storing the object answers the retained request and completes the read.
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store_watch
success (job recv --tag $push --timeout 10sec) "the push should complete"
let output = job recv --tag $read --timeout 10sec
assert equal ($output.data.value.bytes | decode base64 | decode utf-8) 'hello' "the read should complete after the object arrives"
