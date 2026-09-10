use ../../test.nu *

# An acknowledged request to await indexing must survive its acknowledgment timeout without being replaced.
let server = server spawn --config {
	advanced: { checkpoints: true, single_process: false },
	indexer: { request: { timeout: 0.05 } },
}
let receive_watch = tg checkpoint watch indexer.request.receive | from json | get watch
let wait_watch = tg checkpoint watch indexer.request.wait | from json | get watch
let url = $server.url
let request = job spawn {
	let id = job id
	let output = tg --url $url index | complete
	$output | job send --tag $id 0
}

tg checkpoint wait indexer.request.receive $receive_watch 0 | ignore
tg checkpoint continue indexer.request.receive $receive_watch 0
tg checkpoint wait indexer.request.wait $wait_watch 0 | ignore
sleep 1500ms
tg checkpoint unwatch indexer.request.wait $wait_watch

# Any replacement request would be held at the next receive checkpoint.
let output = job recv --tag $request --timeout 10sec
success $output "the original acknowledged request must finish"
tg checkpoint unwatch indexer.request.receive $receive_watch
