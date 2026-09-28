use ../lib/test.nu *

# Sync authorization survives the control task and requires the bearer token for another user.
let root_token = random chars
let remote_destination = server spawn --name remote-destination --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { control: { attempt_ttl: 0.1, recovery_timeout: 0.1, request_timeout: 0.1 } },
}
let alice = tg --url $remote_destination.url login --verbose --name alice | from json
let bob = tg --url $remote_destination.url login --verbose --name bob | from json
let local_source = server spawn --name local-source --config {
	remotes: { default: { token: $alice.token, url: $remote_destination.url } },
}
let file = tg --url $local_source.url put --no-tokens 'tg.file("private")' | str trim
tg --url $remote_destination.url --token $root_token index
let batch_watch = tg --url $remote_destination.url --token $root_token checkpoint watch index.batch | from json | get watch
let stopped_watch = tg --url $remote_destination.url --token $root_token checkpoint watch sync.control.stopped | from json | get watch
let referent = tg --url $local_source.url push $file | str trim
let referent = $referent | str replace --all 'tokens[remote]' 'tokens[local]'
let proof = $'http://localhost/($referent)' | url parse | get params | where {|param| $param.key =~ '^tokens\[' } | each {|param|
	let body = $param.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
	{ body: $body, token: $param.value }
} | where {|proof| $proof.body.resource | str starts-with 'syn_' } | first
let body = $proof.body
let query = $'tokens[local][0]=($proof.token | url encode --all)'
let referent = $'($file)?($query)'
assert ($body.resource | str starts-with 'syn_') 'the token should identify a sync'
assert equal $body.permissions [sync_read]
# Leave the sync batch queued until a read reaches authorization's indexing wait.
tg --url $remote_destination.url --token $root_token checkpoint wait index.batch $batch_watch 0 | ignore
tg --url $remote_destination.url --token $root_token checkpoint wait sync.control.stopped $stopped_watch 0 | ignore
tg --url $remote_destination.url --token $root_token checkpoint unwatch sync.control.stopped $stopped_watch
let wait_watch = tg --url $remote_destination.url --token $root_token checkpoint watch authorization.index.wait | from json | get watch
let read = job spawn {
	let job_id = job id
	let output = tg --url $remote_destination.url --token $bob.token read $referent | complete
	$output | job send --tag $job_id 0
}
tg --url $remote_destination.url --token $root_token checkpoint wait authorization.index.wait $wait_watch 0 | ignore
assert equal (try { job recv --tag $read --timeout 0sec } catch { null }) null 'the read should wait for indexing'
tg --url $remote_destination.url --token $root_token checkpoint unwatch authorization.index.wait $wait_watch
tg --url $remote_destination.url --token $root_token checkpoint unwatch index.batch $batch_watch
let output = job recv --tag $read --timeout 10sec
success $output 'authorization should wait for the queued sync grants'
assert equal $output.stdout 'private'
success (tg --url $remote_destination.url --token $alice.token read $file | complete) 'the caller should retain access through the sync grant'
failure (tg --url $remote_destination.url --token $bob.token read $file | complete) 'storage alone should not authorize another user'
assert equal (tg --url $remote_destination.url --token $bob.token read $referent) 'private'
