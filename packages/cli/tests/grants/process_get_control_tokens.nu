use ../lib/test.nu *

# Node-authorized control reads return output tokens that authorize reading the output once its pending transfer completes.

# Pass the output referent's tokens down to the output object, as a client does.
def output-referent [result: record] {
	let node = $result.data.output.node.value
	let offset = $node | referent tokens local | length
	let separator = if ($node | str contains '?') { '&' } else { '?' }
	let query = $result.data.output.options.tokens.local | enumerate | each { |entry|
		$'tokens[local][($offset + $entry.index)]=($entry.item | url encode --all)'
	} | str join '&'
	$'($node)($separator)($query)'
}

let root = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false }
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
	roles: [api indexer scheduler]
}
let created = tg --url $remote.url --token $root runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true }
	remotes: { default: { token: $created.token.token, url: $remote.url } }
	roles: [api indexer runner]
	runner: { id: $created.data.id, remote: default, token: $created.token.token }
}
let reader = tg --url $remote.url login --verbose --name reader | from json
let output_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch
let path = artifact { tangram.ts: 'export default () => tg.file("control-sync-output");' }
let spawned = tg --url $remote.url --token $root build --detach --verbose $path | from json
let process = $spawned.process | referent node
timeout 30s tg --url $runner.url checkpoint wait runner.process.outcome.sync.started $output_watch 0 | ignore
tg --url $remote.url --token $root wait --source=runner $process | ignore
tg --url $remote.url --token $root grant $reader.user.id process_node $process | ignore
tg --url $remote.url --token $root index
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let result = http get --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)?source=runner'
let file = output-referent $result
let pull = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $reader.token cat $file | complete
	$output | job send --tag $job_id 0
}
let pending = try { job recv --tag $pull --timeout 1sec } catch { null }
assert equal $pending null 'the pull must wait for the pending object transfer'
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $output_watch
let output = job recv --tag $pull --timeout 30sec
success $output 'the output tokens must authorize the pull after the transfer completes'
assert equal $output.stdout 'control-sync-output'

# The tokens returned by an index read must also authorize reading the output.
let indexed = http get --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)?source=index'
let output = tg --url $remote.url --token $reader.token cat (output-referent $indexed) | complete
success $output 'the indexed output tokens must authorize the pull'
assert equal $output.stdout 'control-sync-output'
