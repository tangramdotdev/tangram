use ../../test.nu *

# An eager push sends its root object before the remote returns the sync response headers.

let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}
let file = tg --url $local.url put 'tg.file("hello")' | str trim
let response_watch = (
	tg --url $remote.url checkpoint watch sync.request.response
	| from json
	| get watch
)
let object_watch = (
	tg --url $remote.url checkpoint watch sync.get.input.object
	| from json
	| get watch
)

let push = job spawn {
	let job_id = job id
	let output = tg --url $local.url push $file | complete
	$output | job send --tag $job_id 0
}

tg --url $remote.url checkpoint wait sync.request.response $response_watch 0 | ignore
tg --url $remote.url checkpoint wait sync.get.input.object $object_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.get.input.object $object_watch 0
tg --url $remote.url checkpoint unwatch sync.get.input.object $object_watch
tg --url $remote.url checkpoint continue sync.request.response $response_watch 0
tg --url $remote.url checkpoint unwatch sync.request.response $response_watch

success (job recv --tag $push --timeout 10sec)
