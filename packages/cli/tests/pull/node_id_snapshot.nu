use ../../test.nu *

# A sync retries when a conflicting ID is created after authorization.

let source = spawn --cloud --name source
let incoming = tg --url $source.url group create race | from json
let destination = spawn --name destination --config {
	advanced: { checkpoints: true }
	remotes: { default: { url: $source.url } }
}
let watch = tg --url $destination.url checkpoint watch sync.get.database.authorized | from json | get watch
let pull = job spawn {
	let job_id = job id
	let output = tg --url $destination.url pull --force race | complete
	$output | job send --tag $job_id 0
}

tg --url $destination.url checkpoint wait sync.get.database.authorized $watch 0 | ignore
let existing = tg --url $destination.url group create race | from json
tg --url $destination.url checkpoint continue sync.get.database.authorized $watch 0
tg --url $destination.url checkpoint unwatch sync.get.database.authorized $watch

let output = job recv --tag $pull --timeout 10sec
assert equal $output.exit_code 0 "the sync should retry with the changed ID snapshot"
assert equal (tg --url $destination.url group get race | from json | get id) $incoming.id
failure (tg --url $destination.url group get --location='local' $existing.id | complete) "the replaced group should be deleted"
