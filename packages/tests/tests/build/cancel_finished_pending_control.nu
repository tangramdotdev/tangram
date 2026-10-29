use ../../util/test.nu *

# Cancellation observes completion after its first index lookup while control is pending.

let root_token = random chars
let local_owner = server spawn --name local-owner --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $local_owner.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $root_token, url: $local_owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let finish_watch = tg --url $runner.url checkpoint watch runner.process.finish | from json | get watch
let index_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.cancel.index | from json | get watch
let control_watch = tg --url $local_owner.url --token $root_token checkpoint watch process.cancel.control | from json | get watch
let path = artifact { tangram.ts: 'export default () => "done";' }
let id = checkin push $local_owner $path --token $root_token
let spawned = tg --url $local_owner.url --token $root_token build --detach --verbose $id | from json
success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.finish $finish_watch 0 | complete)
let cancel = job spawn {
	let job_id = job id
	let output = tg --url $local_owner.url --token $root_token cancel $spawned.process $spawned.lease | complete
	$output | job send --tag $job_id 0
}
success (tg --url $local_owner.url --token $root_token checkpoint wait --timeout 10s process.cancel.index $index_watch 0 | complete)
success (tg --url $local_owner.url --token $root_token checkpoint wait --timeout 10s process.cancel.control $control_watch 0 | complete)
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch
success (timeout 30s tg --url $local_owner.url --token $root_token wait $spawned.process | complete)
tg --url $local_owner.url --token $root_token index
tg --url $local_owner.url --token $root_token checkpoint unwatch process.cancel.index $index_watch
let output = job recv --tag $cancel --timeout 5sec
success $output "cancellation must observe completion without the pending control request"
tg --url $local_owner.url --token $root_token checkpoint unwatch process.cancel.control $control_watch
