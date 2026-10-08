use ../lib/test.nu *

# An indexed wait returns the authorization token for the process sync while the outcome sync is still blocked.
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

for field in [output error log] {
	let operation = if $field == log { 'get' } else { 'wait' }
	let push_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch
	let source = if $field == output {
		'export default () => tg.file("indexed output");'
	} else if $field == error {
		'export default () => { throw new Error("indexed error"); };'
	} else {
		'export default () => { console.log("indexed log"); return tg.file("indexed output"); };'
	}
	let path = artifact { tangram.ts: $source }
	let spawned = tg --url $remote.url --token $root_token build --detach --verbose $path | from json
	let process = $spawned.process | split row '?' | first
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.outcome.sync.started $push_watch 0 | complete) "the outcome sync must be held"
	tg --url $remote.url --token $root_token grant $alice.user.id process_node,process_node_output_objects,process_node_error_objects,process_node_log_objects $process | ignore
	tg --url $remote.url --token $root_token grant $bob.user.id process_node $process | ignore
	tg --url $remote.url --token $root_token index

	timeout 30s tg --url $remote.url --token $root_token wait --source=index $process | ignore
	# Force the index path so a live runner response cannot supply the token.
	let output = timeout 30s tg --url $remote.url --token $root_token $operation --source=index $process | from json
	let object = if $field == output { $output.output.value } else if $field == error { $output.error } else { $output.log }
	let params = $'http://localhost/($object)' | url parse | get params
	assert ($params | where {|param| $param.key starts-with 'tokens[' } | any {|param|
		let body = $param.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
		($body.resource | str starts-with 'syn_') and ('sync_read' in $body.permissions)
	}) "the indexed outcome must carry the authorization token for the sync"

	# Hold the control response so an automatic wait must return the indexed outcome.
	let control_watch = tg --url $remote.url --token $root_token checkpoint watch process.get.control | from json | get watch
	let automatic = timeout 10s tg --url $remote.url --token $root_token $operation $process | from json
	assert equal $automatic $output "an automatic wait must retain the indexed tokens"
	tg --url $remote.url --token $root_token checkpoint unwatch process.get.control $control_watch

	let node_output_objects = timeout 10s tg --url $remote.url --token $bob.token $operation --source=index $process | from json
	let node_object = if $field == output { $node_output_objects.output.value } else if $field == error { $node_output_objects.error } else { $node_output_objects.log }
	if $field == log {
		assert ($node_object | str contains 'tokens') "get must preserve the stored authorization token for the sync with node permission"
	} else {
		assert (not ($node_object | str contains 'tokens')) "node permission must not expose authorization tokens for the process sync in wait"
	}

	if $field == output {
		tg --url $remote.url --token $root_token grant $bob.user.id process_node_output_objects $process | ignore
		tg --url $remote.url --token $root_token index
		let partial = timeout 10s tg --url $remote.url --token $bob.token $operation --source=index $process | from json
		assert ($partial.output.value | str contains 'tokens') "output permission must expose the authorization token for the outcome sync"
	}


	if $field == error {
		tg --url $remote.url --token $root_token grant $bob.user.id process_node_error_objects $process | ignore
		tg --url $remote.url --token $root_token index
		let partial = timeout 10s tg --url $remote.url --token $bob.token $operation --source=index $process | from json
		assert ($partial.output? == null) "an error-only outcome must not contain output objects"
		assert ($partial.error | str contains 'tokens') "error permission must expose the authorization token for the outcome sync"
	}

	let authorized = timeout 10s tg --url $remote.url --token $alice.token $operation --source=index $process | from json
	let authorized_object = if $field == output { $authorized.output.value } else if $field == error { $authorized.error } else { $authorized.log }
	assert ($authorized_object | str contains 'tokens') "field permission must expose the authorization token for the process sync"
	tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $push_watch
	let read = timeout 30s tg --url $remote.url --token $alice.token get $authorized_object | complete
	success $read "the indexed authorization token for the sync must allow reading the result"
}
