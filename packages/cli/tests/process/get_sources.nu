use ../lib/test.nu *

# Live control requests do not wait for the index; finished process gets do so to preserve finished logs.

let root_token = random chars
let local_owner = server spawn --name local-owner --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $local_owner.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token } },
	remotes: { default: { token: $root_token, url: $local_owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let finish_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.finish | from json | get watch
let retention_watch = tg --url $runner.url --token $root_token checkpoint watch runner.process.control.retention.finished | from json | get watch
let path = artifact { tangram.ts: 'export default () => { console.log("finished log"); return "done"; };' }
let process = tg --url $local_owner.url --token $root_token build --no-tokens --detach $path | referent node
timeout 30s tg --url $runner.url --token $root_token checkpoint wait runner.process.finish $finish_watch 0 | ignore
let params = { process: $process } | to json --raw

# Hold the index response, forcing each operation to use its live control response.
for operation in [get status children] {
	let watch = tg --url $local_owner.url --token $root_token checkpoint watch process.get.index --params $params | from json | get watch
	let get_job = job spawn {
		let job_id = job id
		let output = tg --url $local_owner.url --token $root_token process $operation $process | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $local_owner.url --token $root_token checkpoint wait process.get.index $watch 0 | ignore
	let output = job recv --tag $get_job --timeout 10sec
	success $output $'a live ($operation) must not wait for the index'
	let output = $output.stdout | from json
	if $operation == get { assert equal $output.status started }
	if $operation == status { assert equal $output [started] }
	if $operation == children { assert equal $output [] }
	tg --url $local_owner.url --token $root_token checkpoint unwatch process.get.index $watch
}

# Retain finished control state while the owner transfers its finished log.
tg --url $runner.url --token $root_token checkpoint unwatch runner.process.finish $finish_watch
timeout 30s tg --url $runner.url --token $root_token checkpoint wait runner.process.control.retention.finished $retention_watch 0 | ignore
tg --url $local_owner.url --token $root_token index
let indexed = tg --url $local_owner.url --token $root_token get $process | from json
assert ($indexed.log? | is-not-empty) "the finished index must contain a finished log"

# Status and wait use finished control data without waiting for the index.
for operation in [status wait] {
	let watch = tg --url $local_owner.url --token $root_token checkpoint watch process.get.index --params $params | from json | get watch
	let get_job = job spawn {
		let job_id = job id
		let output = tg --url $local_owner.url --token $root_token process $operation $process | complete
		$output | job send --tag $job_id 0
	}
	timeout 10s tg --url $local_owner.url --token $root_token checkpoint wait process.get.index $watch 0 | ignore
	let output = job recv --tag $get_job --timeout 10sec
	success $output $'a finished ($operation) must not wait for the index'
	let output = $output.stdout | from json
	if $operation == status { assert equal $output [finished] }
	if $operation == wait {
		assert equal $output.exit 0
		assert equal $output.output done
	}
	tg --url $local_owner.url --token $root_token checkpoint unwatch process.get.index $watch
}

# A finished control response must wait for the indexed record before answering a full get.
let watch = tg --url $local_owner.url --token $root_token checkpoint watch process.get.index --params $params | from json | get watch
let response_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.control.response.published --params ({ kind: get, process: $process } | to json --raw) | from json | get watch
let get_job = job spawn {
	let job_id = job id
	let output = tg --url $local_owner.url --token $root_token get $process | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $local_owner.url --token $root_token checkpoint wait process.get.index $watch 0 | ignore
timeout 10s tg --url $local_owner.url --token $root_token checkpoint wait process.control.response.published $response_watch 0 | ignore
let premature = try { job recv --tag $get_job --timeout 200ms } catch { null }
assert equal $premature null "a finished control response must consult the index"
tg --url $local_owner.url --token $root_token checkpoint unwatch process.get.index $watch
tg --url $local_owner.url --token $root_token checkpoint unwatch process.control.response.published $response_watch
let output = job recv --tag $get_job --timeout 10sec
success $output
assert equal ($output.stdout | from json | get log | referent node) ($indexed.log | referent node)

# Retained local runner state must consult the owner for the finished log too.
let output = tg --url $runner.url --token $root_token get --remote $process | from json
assert equal ($output.log | referent node) ($indexed.log | referent node)
let log = tg --url $local_owner.url --token $root_token process log $process | str trim
assert equal $log 'finished log'
tg --url $runner.url --token $root_token checkpoint unwatch runner.process.control.retention.finished $retention_watch
