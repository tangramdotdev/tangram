use ../../test.nu *

# A tag deleted after it is read is returned from the consistent snapshot rather than causing an internal error.

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
	tg checkpoint watch tag.get.read --params ({ id: $tag.id } | to json)
	| from json
	| get watch
)
let get = job spawn {
	let job_id = job id
	let output = tg tag get $tag.id | complete
	$output | job send --tag $job_id 0
}

tg checkpoint wait tag.get.read $watch 0 | ignore
tg tag delete test
tg checkpoint continue tag.get.read $watch 0
tg checkpoint unwatch tag.get.read $watch

let output = job recv --tag $get --timeout 10sec
success $output
let output = $output.stdout | from json
assert equal $output.id $tag.id "the tag get should return the snapshot"
assert equal $output.specifier $tag.specifier "the tag get should return the snapshot"
