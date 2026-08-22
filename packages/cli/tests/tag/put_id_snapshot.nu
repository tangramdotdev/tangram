use ../../test.nu *

# A tag put retries when a parent is created after authorization.

let server = spawn --config {
	advanced: { checkpoints: true }
}
let path = artifact 'data'
let node = tg checkin $path | str trim
let watch = (
	tg checkpoint watch tag.put.authorized --params ({ specifier: 'parent/tag' } | to json)
	| from json
	| get watch
)
let put = job spawn {
	let job_id = job id
	let output = tg tag put --no-pull-ancestors parent/tag $node | complete
	$output | job send --tag $job_id 0
}

tg checkpoint wait tag.put.authorized $watch 0 | ignore
let parent = tg group create parent | from json
tg checkpoint continue tag.put.authorized $watch 0
tg checkpoint unwatch tag.put.authorized $watch

let output = job recv --tag $put --timeout 10sec
assert equal $output.exit_code 0 "the tag put should retry with the new parent ID"
assert equal (tg group get parent | from json | get id) $parent.id
assert equal (tg tag get parent/tag | from json | get specifier) 'parent/tag'
