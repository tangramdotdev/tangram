use ../../test.nu *

# An eager push sends its root process before the remote returns the sync response headers.

let remote = spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
}
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}
let path = artifact {
	tangram.ts: 'export default function () { return "hello" }'
}
let process = tg --url $local.url build --detach $path | str trim
tg --url $local.url wait $process
tg --url $local.url index
let response_watch = (
	tg --url $remote.url checkpoint watch sync.request.response
	| from json
	| get watch
)
let process_watch = (
	tg --url $remote.url checkpoint watch sync.get.input.process
	| from json
	| get watch
)

let push = job spawn {
	let job_id = job id
	let output = tg --url $local.url push $process | complete
	$output | job send --tag $job_id 0
}

tg --url $remote.url checkpoint wait sync.request.response $response_watch 0 | ignore
tg --url $remote.url checkpoint wait sync.get.input.process $process_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.get.input.process $process_watch 0
tg --url $remote.url checkpoint unwatch sync.get.input.process $process_watch
tg --url $remote.url checkpoint continue sync.request.response $response_watch 0
tg --url $remote.url checkpoint unwatch sync.request.response $response_watch

success (job recv --tag $push --timeout 10sec)
