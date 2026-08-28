use ../../test.nu *

# Pushing a large graph to an authenticated region in a multi-region cloud
# instance completes under bidirectional backpressure.

skip_if_no_cloud
let region_a_port = port
let region_b_port = port ($region_a_port + 1)
let region_a_url = $'http://127.0.0.1:($region_a_port)'
let region_b_url = $'http://127.0.0.1:($region_b_port)'
let regions = [
	{ name: a, url: $region_a_url },
	{ name: b, url: $region_b_url },
]
let instance = instance --cloud --primary-region a --regions $regions --config {
	authentication: { users: { providers: { insecure: true } } },
}
let region_a = server spawn --instance $instance --region a --preserve-keys --name region-a --url $region_a_url
let region_b = server spawn --instance $instance --region b --preserve-keys --name region-b --url $region_b_url
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
