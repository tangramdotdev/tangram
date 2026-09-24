use ../lib/test.nu *

# Cancellation observes completion after its first index lookup while control is pending.

let root_token = random chars
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $owner.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $root_token, url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let finish_watch = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
let index_watch = tg --url $owner.url --token $root_token checkpoint watch process.cancel.index | from json | get watch
let control_watch = tg --url $owner.url --token $root_token checkpoint watch process.cancel.control | from json | get watch
let path = artifact { tangram.ts: 'export default () => "done";' }
let spawned = tg --url $owner.url --token $root_token build --detach --verbose $path | from json
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 0 | complete)
let cancel = job spawn {
	let job_id = job id
	let output = tg --url $owner.url --token $root_token cancel $spawned.process $spawned.lease | complete
	$output | job send --tag $job_id 0
}
success (timeout 10s tg --url $owner.url --token $root_token checkpoint wait process.cancel.index $index_watch 0 | complete)
success (timeout 10s tg --url $owner.url --token $root_token checkpoint wait process.cancel.control $control_watch 0 | complete)
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch
success (timeout 30s tg --url $owner.url --token $root_token wait $spawned.process | complete)
tg --url $owner.url --token $root_token index
tg --url $owner.url --token $root_token checkpoint unwatch process.cancel.index $index_watch
let output = job recv --tag $cancel --timeout 5sec
success $output "cancellation must observe completion without the pending control request"
tg --url $owner.url --token $root_token checkpoint unwatch process.cancel.control $control_watch
