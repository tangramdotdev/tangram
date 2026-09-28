use ../lib/test.nu *

# A sync rechecks an existing tag target after authorization.

let local_source = server spawn --cloud --name local-source
let source_target = tg --url $local_source.url put 'tg.file("source")' | str trim
tg --url $local_source.url tag put race $source_target
let remote_destination = server spawn --name remote-destination --config {
	advanced: { checkpoints: true }
	remotes: { default: { url: $local_source.url } }
}
tg --url $remote_destination.url pull race

let destination_target = tg --url $remote_destination.url put 'tg.file("destination")' | str trim
let watch = tg --url $remote_destination.url checkpoint watch sync.get.database.authorized | from json | get watch
let pull = job spawn {
	let job_id = job id
	let output = tg --url $remote_destination.url pull race | complete
	$output | job send --tag $job_id 0
}

tg --url $remote_destination.url checkpoint wait sync.get.database.authorized $watch 0 | ignore
tg --url $remote_destination.url tag put --force race $destination_target
tg --url $remote_destination.url checkpoint continue sync.get.database.authorized $watch 0
tg --url $remote_destination.url checkpoint unwatch sync.get.database.authorized $watch

let output = job recv --tag $pull --timeout 10sec
failure $output "the pull should recheck the changed tag target"
assert ($output.stderr | str contains "the tag already has a different target")
let tag = tg --url $remote_destination.url tag get race | from json
assert equal $tag.target.id $destination_target "the failed pull should preserve the destination tag"
