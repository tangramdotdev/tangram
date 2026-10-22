use ../lib/test.nu *

# A read that waits on an incoming sync completes once the object arrives, even when its first sync service requests are lost.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token } },
	sync: { service: { retry_interval: 0.1 } },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}
let blob = tg --url $local.url put --no-tokens 'tg.blob("hello")' | referent node
let params = { node: $blob } | to json --raw
let subscribe_watch = tg --url $remote.url --token $root_token checkpoint watch sync.service.subscribe | from json | get watch
let retain_watch = tg --url $remote.url --token $root_token checkpoint watch sync.service.request.retain --params $params | from json | get watch
let store_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json --raw) | from json | get watch
let request_watch = tg --url $remote.url --token $root_token checkpoint watch sync.service.request --params $params | from json | get watch

# Hold the incoming object and the service subscription so the first requests are lost.
let push_log = $env.TMPDIR | path join push.log
'' | save --force $push_log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $local.url push $blob o+e>| tee { save --append $push_log } | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $store_watch 0 | ignore
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.service.subscribe $subscribe_watch 0 | ignore
wait_for_log { log: $push_log } 'tokens[remote][0]'
let referent = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[remote\]' } | first | str trim | str replace --regex '^info ' ''
let sync = $'http://localhost/($referent)' | url parse | get params | where key == 'tokens[remote][0]' | first | get value
let query = { 'tokens[local][0]': $sync } | url build-query
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let read = job spawn {
	let job_id = job id
	let headers = { Authorization: $'Bearer ($root_token)' }
	let output = http get --max-time 10sec --unix-socket $socket --headers ($headers | insert Accept 'application/json') $'http://localhost/objects/($blob)?($query)'
	$output | job send --tag $job_id 0
}

# Wait for the missing authorized read to send a request and a retry through the incoming sync before the service starts listening.
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.service.request $request_watch 0 | ignore
let retry_watch = tg --url $remote.url --token $root_token checkpoint watch sync.service.request --params $params | from json | get watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.service.request $request_watch
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.service.request $retry_watch 0 | ignore
tg --url $remote.url --token $root_token checkpoint unwatch sync.service.request $retry_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.service.subscribe $subscribe_watch

# Let the service listen, and check that the read still waits for the held object.
tg --url $remote.url --token $root_token checkpoint unwatch sync.service.request.retain $retain_watch
let premature = try { job recv --tag $read --timeout 1sec } catch { null }
assert equal $premature null "the read should wait for the object"

# Storing the object answers the retained request and completes the read.
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $store_watch
success (job recv --tag $push --timeout 10sec) "the push should complete"
let output = job recv --tag $read --timeout 10sec
assert equal ($output.data.value.bytes | decode base64 | decode utf-8) 'hello' "the read should complete after the object arrives"
