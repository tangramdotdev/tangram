use ../../test.nu *

# A pull sends its first get node before the remote returns the sync response headers.

let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}
let file = tg --url $remote.url put 'tg.file("hello")' | str trim
let response_watch = (
	tg --url $remote.url checkpoint watch sync.request.response
	| from json
	| get watch
)
let node_watch = (
	tg --url $remote.url checkpoint watch sync.put.input.node
	| from json
	| get watch
)

let pull = job spawn {
	let job_id = job id
	let output = tg --url $local.url pull $file | complete
	$output | job send --tag $job_id 0
}

tg --url $remote.url checkpoint wait sync.request.response $response_watch 0 | ignore
tg --url $remote.url checkpoint wait sync.put.input.node $node_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.put.input.node $node_watch 0
tg --url $remote.url checkpoint unwatch sync.put.input.node $node_watch
tg --url $remote.url checkpoint continue sync.request.response $response_watch 0
tg --url $remote.url checkpoint unwatch sync.request.response $response_watch

success (job recv --tag $pull --timeout 10sec)
