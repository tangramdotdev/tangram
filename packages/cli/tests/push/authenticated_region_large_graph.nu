use ../../test.nu *

# Pushing a large graph to an authenticated region in a multi-region cloud
# instance completes under bidirectional backpressure.

skip_if_no_cloud
let regions = [
	{ name: a },
	{ name: b },
]
let instance = instance --cloud --primary-region a --regions $regions --config {
	authentication: { users: { providers: { insecure: true } } },
}
let region_a = server spawn --instance $instance --region a --preserve-keys --name region-a --url (instance region url $instance a)
let region_b = server spawn --instance $instance --region b --preserve-keys --name region-b --url (instance region url $instance b)
assert ($instance.directory? == null) 'a cloud instance must not own a directory'
assert ($region_a.directory != $region_b.directory) 'cloud servers must own separate directories'
let alice = tg --url $region_a.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $region_b.url } },
}

# Create enough distinct files to exercise backpressure in both directions.
let path = mktemp -d | path join source
mkdir $path
for directory_index in 0..<50 {
	let child = $path | path join $'directory_($directory_index)'
	mkdir $child
	for file_index in 0..<40 {
		$'contents ($directory_index) ($file_index)' | save ($child | path join $'file_($file_index).txt')
	}
}
let directory = tg --url $local.url checkin $path | str trim
tg --url $local.url index
let metadata = tg --url $local.url object metadata $directory | from json
assert ($metadata.subtree.count > 4000)

let push = job spawn {
	let job_id = job id
	let output = tg --url $local.url push $directory | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $push --timeout 10sec } catch { null }
if $output == null {
	error make { msg: 'the push timed out' }
}
success $output

tg --url $region_b.url index
let availability = tg --url $region_b.url --token $alice.token object availability $directory --local | from json
assert equal $availability.subtree true
