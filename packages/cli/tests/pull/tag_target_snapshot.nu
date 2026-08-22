use ../../test.nu *

# A sync rechecks an existing tag target after authorization.

let source = spawn --cloud --name source
let source_target = tg --url $source.url put 'tg.file("source")' | str trim
tg --url $source.url tag put race $source_target
let destination = spawn --name destination --config {
	advanced: { checkpoints: true }
	remotes: { default: { url: $source.url } }
}
tg --url $destination.url pull race

let destination_target = tg --url $destination.url put 'tg.file("destination")' | str trim
let watch = tg --url $destination.url checkpoint watch sync.get.database.authorized | from json | get watch
let pull = job spawn {
	let job_id = job id
	let output = tg --url $destination.url pull race | complete
	$output | job send --tag $job_id 0
}

tg --url $destination.url checkpoint wait sync.get.database.authorized $watch 0 | ignore
tg --url $destination.url tag put --force race $destination_target
tg --url $destination.url checkpoint continue sync.get.database.authorized $watch 0
tg --url $destination.url checkpoint unwatch sync.get.database.authorized $watch

let output = job recv --tag $pull --timeout 10sec
failure $output "the pull should recheck the changed tag target"
assert ($output.stderr | str contains "the tag already has a different target")
let tag = tg --url $destination.url tag get race | from json
assert equal $tag.target.id $destination_target "the failed pull should preserve the destination tag"
