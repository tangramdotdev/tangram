use ../../test.nu *

# Runner gets fetch metadata and availability concurrently, preserving the requested region for those reads.

let root_token = random chars
let instance = instance --primary-region a --regions [{ name: a }]
let remote = server spawn --instance $instance --region a --url (instance region url $instance a) --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let reader = tg --url $runner.url login --verbose --name reader | from json
tg --url $runner.url --token $reader.token remote put default $remote.url
let finish_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.finish | from json | get watch
let path = artifact { tangram.ts: 'export default () => "output";' }
let process = tg --url $remote.url --token $root_token build --detach $path | str trim
timeout 30s tg --url $runner.url --token $root_token checkpoint wait runner.process.finish $finish_watch 0 | ignore
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let local = http get --unix-socket $socket --headers { Authorization: $'Bearer ($root_token)' } $'http://localhost/processes/($process)?location=remote'
let remote_socket = $remote.url | str replace 'http+unix://' '' | url decode
let owned = http get --unix-socket $remote_socket --headers { Authorization: $'Bearer ($root_token)' } $'http://localhost/processes/($process)'

# Hold both index-only reads; a sequential implementation cannot reach the second one.
let params = { id: $process } | to json --raw
let metadata_watch = tg --url $remote.url --token $root_token checkpoint watch process.metadata --params $params | from json | get watch
let availability_watch = tg --url $remote.url --token $root_token checkpoint watch process.availability --params $params | from json | get watch
let query = { availability: true, location: 'remote(a)', metadata: true, 'tokens[local][0]': $local.tokens.local.0, 'tokens[remote][0]': $owned.tokens.local.0 } | url build-query
let read_job = job spawn {
	let job_id = job id
	let output = http get --max-time 30sec --unix-socket $socket --headers { Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)?($query)'
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $remote.url --token $root_token checkpoint wait process.metadata $metadata_watch 0 | ignore
timeout 10s tg --url $remote.url --token $root_token checkpoint wait process.availability $availability_watch 0 | ignore
tg --url $remote.url --token $root_token checkpoint unwatch process.metadata $metadata_watch
tg --url $remote.url --token $root_token checkpoint unwatch process.availability $availability_watch
let output = job recv --tag $read_job --timeout 10sec
assert equal $output.location remote
assert equal $output.data.status started
assert ($output.metadata? | is-empty) "metadata remains unavailable until the owning index has finished process data"
assert ('availability' in ($output | columns))

# A nonexistent region is ignored for the runner data, but still fails when an index-only field needs routing.
let query = { location: 'remote(missing)', metadata: true, 'tokens[local][0]': $local.tokens.local.0, 'tokens[remote][0]': $owned.tokens.local.0 } | url build-query
let output = http get --allow-errors --full --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)?($query)'
assert equal $output.status 500
tg --url $runner.url --token $root_token checkpoint unwatch runner.process.finish $finish_watch
