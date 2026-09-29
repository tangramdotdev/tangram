use ../lib/test.nu *

# An indexed wait returns the authorization token for the output sync while the output push is still blocked.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	process: { await_push: false },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

for field in [output error] {
	let push_watch = tg --url $runner.url checkpoint watch runner.process.output.push.started | from json | get watch
	let source = if $field == output {
		'export default () => tg.file("indexed output");'
	} else {
		'export default () => { throw new Error("indexed error"); };'
	}
	let path = artifact { tangram.ts: $source }
	let spawned = tg --url $remote.url --token $root_token build --detach --verbose $path | from json
	let process = $spawned.process | split row '?' | first
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.started $push_watch 0 | complete) "the result push must be held"
	tg --url $remote.url --token $root_token grant $alice.user.id process_node,process_node_output,process_node_error $process | ignore
	tg --url $remote.url --token $root_token grant $bob.user.id process_node $process | ignore
	tg --url $remote.url --token $root_token index

	# Force the index path so a live runner response cannot supply the token.
	let output = timeout 30s tg --url $remote.url --token $root_token wait --source=index $process | from json
	let object = if $field == output { $output.output.value } else { $output.error }
	let params = $'http://localhost/($object)' | url parse | get params
	assert ($params | where {|param| $param.key starts-with 'tokens[' } | any {|param|
		let body = $param.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
		($body.resource | str starts-with 'syn_') and ('sync_read' in $body.permissions)
	}) "the indexed result must carry the authorization token for the sync"

	# Hold the control response so an automatic wait must return the indexed result.
	let control_watch = tg --url $remote.url --token $root_token checkpoint watch process.get.control | from json | get watch
	let automatic = timeout 10s tg --url $remote.url --token $root_token wait $process | from json
	assert equal $automatic $output "an automatic wait must retain the indexed tokens"
	tg --url $remote.url --token $root_token checkpoint unwatch process.get.control $control_watch

	let node_output = timeout 10s tg --url $remote.url --token $bob.token wait --source=index $process | from json
	let node_object = if $field == output { $node_output.output.value } else { $node_output.error }
	assert (not ($node_object | str contains 'tokens')) "node permission must not expose authorization tokens for the output sync"

	if $field == error {
		tg --url $remote.url --token $root_token grant $bob.user.id process_node_error $process | ignore
		tg --url $remote.url --token $root_token index
		let partial = timeout 10s tg --url $remote.url --token $bob.token wait --source=index $process | from json
		assert (not ($partial.error | str contains 'tokens')) "error permission alone must not expose a sync covering both fields"
	}

	let authorized = timeout 10s tg --url $remote.url --token $alice.token wait --source=index $process | from json
	let authorized_object = if $field == output { $authorized.output.value } else { $authorized.error }
	assert ($authorized_object | str contains 'tokens') "field permission must expose the authorization token for the output sync"
	tg --url $runner.url checkpoint unwatch runner.process.output.push.started $push_watch
	let read = timeout 30s tg --url $remote.url --token $alice.token get $authorized_object | complete
	success $read "the indexed authorization token for the sync must allow reading the result"
}
