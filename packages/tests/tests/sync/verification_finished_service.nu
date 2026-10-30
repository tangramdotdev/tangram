use ../../util/test.nu *

# Verification retries the index when a discovered sync finishes before its service can be contacted.
let root_token = random chars
let remote_destination = server spawn --name remote-destination --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { service: { attempt_ttl: 0.1, recovery_timeout: 0.1, request_timeout: 1.0 } },
}
let alice = tg --url $remote_destination.url login --verbose --name alice | from json
let bob = tg --url $remote_destination.url login --verbose --name bob | from json
let local_source = server spawn --name local-source --config {
	remotes: { default: { token: $alice.token, url: $remote_destination.url } },
}
let file = tg --url $local_source.url put --no-tokens 'tg.file("private")' | referent node
tg --url $remote_destination.url --token $root_token index
let batch_watch = tg --url $remote_destination.url --token $root_token checkpoint watch index.write | from json | get watch
let stopped_watch = tg --url $remote_destination.url --token $root_token checkpoint watch sync.service.stopped | from json | get watch
let push_log = $env.TMPDIR | path join push.log
'' | save --force $push_log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $local_source.url push $file o+e>| tee { save --append $push_log } | complete
	$output | job send --tag $job_id 0
}
wait_for_log { log: $push_log } 'tokens[remote][0]'
let referent = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[remote\]' } | first | str trim | str replace --regex '^info ' ''
let referent = $referent | str replace --all 'tokens[remote]' 'tokens[local]'
# Read through only the sync token, so that authorization must wait for the sync's queued permissions.
let token = $'http://localhost/($referent)' | url parse | get params | where {|param| $param.key =~ '^tokens\[' } | get value | where {|token|
	$token | token body | get resource | str starts-with 'syn_'
} | first
let referent = $'($file)?tokens[local][0]=($token | url encode --all)'
# Hold the sync batch until verification has started contacting its service.
tg --url $remote_destination.url --token $root_token checkpoint wait index.write $batch_watch 0 | ignore
let heartbeat_watch = tg --url $remote_destination.url --token $root_token checkpoint watch sync.service.heartbeat.start | from json | get watch
let read = job spawn {
	let job_id = job id
	let output = tg --url $remote_destination.url --token $bob.token read $referent | complete
	$output | job send --tag $job_id 0
}
tg --url $remote_destination.url --token $root_token checkpoint wait sync.service.heartbeat.start $heartbeat_watch 0 | ignore
assert equal (try { job recv --tag $read --timeout 0.2sec } catch { null }) null 'verification should still be waiting for a proof'
tg --url $remote_destination.url --token $root_token checkpoint unwatch index.write $batch_watch
success (job recv --tag $push --timeout 10sec) 'the sync should complete after its final index writes'
tg --url $remote_destination.url --token $root_token checkpoint wait sync.service.stopped $stopped_watch 0 | ignore
tg --url $remote_destination.url --token $root_token checkpoint unwatch sync.service.stopped $stopped_watch
# The control task is gone before the verifier can send its first heartbeat.
tg --url $remote_destination.url --token $root_token checkpoint unwatch sync.service.heartbeat.start $heartbeat_watch
let output = job recv --tag $read --timeout 10sec
success $output 'verification should retry the index after the service timeout'
assert equal $output.stdout 'private'
success (tg --url $remote_destination.url --token $alice.token read $file | complete) 'the caller should retain access through the sync permissions'
failure (tg --url $remote_destination.url --token $bob.token read $file | complete) 'storage alone should not authorize another user'
assert equal (tg --url $remote_destination.url --token $bob.token read $referent) 'private'
