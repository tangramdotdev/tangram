use ../../test.nu *

# Runner reads precede remote dispatch without changing the process's location or token issuer.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	process: { status_wakeup_interval: 3600.0 },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let finish_watch = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
let remote_finish_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.finish | from json | get watch
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("runner output"); }',
}
let spawned = tg --url $local.url build --remote --detach --verbose $path | from json
let process = $spawned.process | split row '?' | first
let hit = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 0 | from json
assert equal $hit.params.process $process

# Preserve the remote capability alongside any locally issued capability.
let remote_socket = $remote.url | str replace 'http+unix://' '' | url decode
let remote_response = http get --unix-socket $remote_socket --headers { Authorization: $'Bearer ($root_token)' } $'http://localhost/processes/($process)'
let remote_token = $remote_response.tokens.local.authorization.0
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let query = { location: 'remote(hint)', 'tokens[remote][authorization][0]': $remote_token } | url build-query
let response = http get --unix-socket $socket $'http://localhost/processes/($process)?($query)'
assert equal $response.location remote "the process still belongs to the remote"
assert equal $response.data.status started
assert equal $response.tokens.remote.authorization.0 $remote_token "the remote capability must retain its issuer"
assert ($response.tokens.local? | is-not-empty) "the runner capability must remain local"

# An explicit local read still means the local index, not the remote process's runner state.
let local_response = http get --unix-socket $socket $'http://localhost/processes/($process)?location=local'
assert equal $local_response.location remote "an indexed copy must preserve the process's remote location"

# Subscribe before the runner finishes, with polling disabled for this test.
let status_job = job spawn {
	let job_id = job id
	let query = { location: 'remote(hint)' } | url build-query
	http get --max-time 30sec --raw --unix-socket $socket $'http://localhost/processes/($process)/status?($query)'
	| lines
	| where { $in in ['data: "started"' 'data: "finished"' 'event: end'] }
	| each { $in | job send --tag $job_id 0 }
	| ignore
}
assert equal (job recv --tag $status_job --timeout 10sec) 'data: "started"'
let params = { process: $process } | to json --raw
let attach_watch = tg --url $runner.url checkpoint watch process.wait.attach --params $params | from json | get watch
let wait_query = { lease: $spawned.lease, location: 'remote(hint)' } | url build-query
let wait_job = job spawn {
	let job_id = job id
	let output = http post --raw --max-time 30sec --unix-socket $socket --headers { Accept: 'text/event-stream' } $'http://localhost/processes/($process)/wait?($wait_query)' ''
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $runner.url checkpoint wait process.wait.attach $attach_watch 0 | ignore
tg --url $runner.url checkpoint unwatch process.wait.attach $attach_watch
let snapshot = tg --url $runner.url process status --remote --timeout 0 $process | from json
assert equal $snapshot [started]

# The remote cannot publish completion until both runner readers have returned.
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 0
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch
let hit = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $remote_finish_watch 0 | from json
assert equal $hit.params.id $process
assert equal (job recv --tag $status_job --timeout 10sec) 'data: "finished"'
assert equal (job recv --tag $status_job --timeout 10sec) 'event: end'
let output = job recv --tag $wait_job --timeout 10sec
let output = $output | lines | where { str starts-with 'data: ' } | last | str substring 6.. | from json
assert equal $output.exit 0
assert ($output.output.value | str contains 'location=local') "the locally available output must stay local"
let contents = tg --url $runner.url cat $output.output.value | str trim
assert equal $contents 'runner output'

# The normal remote path still serves the process after completion is published.
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $remote_finish_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $remote_finish_watch
let output = timeout 30s tg --url $local.url wait --remote $process | complete
success $output "the owning remote must eventually publish completion"
let query = { location: remote, 'tokens[remote][authorization][0]': $remote_token } | url build-query
let response = http get --max-time 10sec --unix-socket $socket $'http://localhost/processes/($process)?($query)'
assert equal $response.location remote
assert equal $response.data.status finished
assert ($response.tokens.remote? | is-not-empty)
assert ($response.tokens.local? | is-empty) "a new finished-process read must use normal remote dispatch"
let cancelled = timeout 10s tg --url $runner.url cancel --remote $process $spawned.lease | complete
success $cancelled "cancelling a finished remote process must use normal dispatch"
