use ../../util/test.nu *

# An enqueue that an indexer accepted before it crashed is sent again after the response timeout, so that the restarted indexer indexes it.

let instance = instance --cloud --config {
	advanced: { checkpoints: true },
	indexer: { request: { response_time_to_live: 1 } },
}
let indexer = server spawn --instance $instance --name indexer --config { roles: [api indexer] }
let api = server spawn --instance $instance --name api --config { roles: [api] }

# Hold the indexer's queue write and processing, so that it accepts the enqueue without answering it.
let write = checkpoint watch indexer.queue.write --url $indexer.url
let process = checkpoint watch index.write --url $indexer.url
let url = $api.url
let put = job spawn {
	let id = job id
	tg --url $url put --no-tokens 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | complete | job send --tag $id 0
}
checkpoint wait $write | ignore
checkpoint wait $process | ignore

# Crash the indexer, and start it again.
kill --signal 9 $indexer.pid
wait_for_exit $indexer.pid
let indexer = server start $indexer

# The put finishes once the restarted indexer receives the enqueue again, and the object is indexed.
let output = job recv --tag $put --timeout 30sec
success $output 'the put should finish after the indexer restarts'
let object = $output.stdout | referent node
tg --url $api.url index
let metadata = tg --url $api.url object metadata $object | from json
assert equal $metadata.subtree.count 5 'the object tree should be indexed'

server stop $api
server stop $indexer
