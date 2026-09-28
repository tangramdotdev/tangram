use ../lib/test.nu *

# A sync retries when a conflicting ID is created after authorization.

let local_source = server spawn --cloud --name local-source
let incoming = tg --url $local_source.url group create race | from json
let remote_destination = server spawn --name remote-destination --config {
	advanced: { checkpoints: true }
	remotes: { default: { url: $local_source.url } }
}
let watch = tg --url $remote_destination.url checkpoint watch sync.get.database.authorized | from json | get watch
let pull = job spawn {
	let job_id = job id
	let output = tg --url $remote_destination.url pull --force race | complete
	$output | job send --tag $job_id 0
}

tg --url $remote_destination.url checkpoint wait sync.get.database.authorized $watch 0 | ignore
let existing = tg --url $remote_destination.url group create race | from json
tg --url $remote_destination.url checkpoint continue sync.get.database.authorized $watch 0
tg --url $remote_destination.url checkpoint unwatch sync.get.database.authorized $watch

let output = job recv --tag $pull --timeout 10sec
assert equal $output.exit_code 0 "the sync should retry with the changed ID snapshot"
assert equal (tg --url $remote_destination.url group get race | from json | get id) $incoming.id
failure (tg --url $remote_destination.url group get --location='local' $existing.id | complete) "the replaced group should be deleted"
