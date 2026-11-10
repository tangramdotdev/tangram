use ../../util/test.nu *

# A stopped indexer is down until it restarts with the same ID, so a wait for indexing finishes only after it restarts and indexes its queued input.
let instance = instance --cloud --config {
	advanced: { checkpoints: true },
	indexer: { cache: { poll_interval: 0.01 } },
}
let local_a = server spawn --instance $instance --name local-a --config { roles: [api indexer] }
let local_api = server spawn --instance $instance --name local-api --config { roles: [api] }

# Queue a batch on A, and stop A as soon as its processing is released.
let batch_watch = tg --url $local_a.url checkpoint watch index.write | from json | get watch
let object = tg --url $local_a.url put --no-tokens 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | referent node
tg --url $local_a.url checkpoint wait index.write $batch_watch 0 | ignore
tg --url $local_a.url checkpoint unwatch index.write $batch_watch
server stop $local_a

# A wait for indexing must not finish while A is down.
let url = $local_api.url
let request = job spawn {
	let id = job id
	let output = tg --url $url index | complete
	$output | job send --tag $id 0
}
let pending = try {
	job recv --tag $request --timeout 1sec | ignore
	false
} catch {
	true
}
assert $pending 'the wait must not finish while the indexer is down'

# The wait must finish after A restarts, and A's input must be indexed.
let local_a = server start $local_a
let output = job recv --tag $request --timeout 30sec
success $output 'the wait must finish after the indexer restarts'
let metadata = tg --url $local_api.url object metadata $object | from json
assert equal $metadata.subtree.count 5 'the restarted indexer must index its queued input'

server stop $local_api
server stop $local_a
