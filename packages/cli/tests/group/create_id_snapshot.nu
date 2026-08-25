use ../../test.nu *

# Group creation retries when an ancestor is created after authorization.

let server = server spawn --config {
	advanced: { checkpoints: true }
}
let watch = (
	tg checkpoint watch group.create.authorized --params ({ specifier: 'parent/child' } | to json)
	| from json
	| get watch
)
let create = job spawn {
	let job_id = job id
	let output = tg group create --create-ancestors parent/child | complete
	$output | job send --tag $job_id 0
}

tg checkpoint wait group.create.authorized $watch 0 | ignore
let parent = tg group create parent | from json
tg checkpoint continue group.create.authorized $watch 0
tg checkpoint unwatch group.create.authorized $watch

let output = job recv --tag $create --timeout 10sec
assert equal $output.exit_code 0 "group creation should retry with the new ancestor ID"
assert equal (tg group get parent | from json | get id) $parent.id
assert equal (tg group get parent/child | from json | get specifier) 'parent/child'
