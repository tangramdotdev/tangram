use ../../test.nu *

# A tag put reauthorizes when a parent is created after authorization.

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let path = artifact 'data'
let target = tg --token $alice.token checkin $path | str trim
let watch = (
	tg --token $root_token checkpoint watch tag.put.authorized --params ({ specifier: 'parent/tag' } | to json)
	| from json
	| get watch
)
let put = job spawn {
	let job_id = job id
	let output = tg --token $alice.token tag put --no-pull-ancestors parent/tag $target | complete
	$output | job send --tag $job_id 0
}

tg --token $root_token checkpoint wait tag.put.authorized $watch 0 | ignore
let parent = tg --token $root_token group create parent | from json
tg --token $root_token checkpoint continue tag.put.authorized $watch 0
tg --token $root_token checkpoint unwatch tag.put.authorized $watch

let output = job recv --tag $put --timeout 10sec
failure $output "the tag put should be reauthorized against the new parent"
assert ($output.stderr | str contains 'unauthorized')
assert equal (tg --token $root_token group get parent | from json | get id) $parent.id
failure (tg --token $root_token tag get parent/tag | complete) "the unauthorized tag should not be created"
