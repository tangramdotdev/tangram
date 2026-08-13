use ../../test.nu *

# A tag deleted after authorization is reported as not found rather than an internal error.

let server = spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let path = artifact 'test'
let id = tg checkin $path
tg tag put test $id
let tag = tg tag get test | from json
let watch = (
	tg checkpoint watch tag.get.authorized --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let get = job spawn {
	let job_id = job id
	let output = tg tag get $tag.id | complete
	$output | job send --tag $job_id 0
}

tg checkpoint wait tag.get.authorized $watch 0 | ignore
tg tag delete test
tg checkpoint continue tag.get.authorized $watch 0
tg checkpoint unwatch tag.get.authorized $watch

let output = job recv --tag $get --timeout 10sec
failure $output
assert not ($output.stderr | str contains 'the request failed') "the tag get should return not found"
