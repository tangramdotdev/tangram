use ../../util/test.nu *
use ../../util/archive.nu *

# An archive item whose put was replaced by a newer put of the same object does not delay the uploads behind it.
skip_test --cloud
let archive = spawn_archive
let local = server spawn --cloud --config {
	advanced: { checkpoints: true, multi_server: true },
	archive: $archive.config,
	object: {
		archive_queue: {
			concurrency: 1,
			retry: { max_delay: 1, max_retries: 1000000 },
		},
	},
}

# Put an object, and hold its archive item before it reads the put from the cache.
let checkpoint = checkpoint watch archive.wait
let output = 0x[00 68 65 6c 6c 6f] | timeout 10 tg object put --no-tokens --bytes --kind blob | complete
success $output
let id = $output.stdout | referent node
checkpoint wait $checkpoint 0

# Put the object again, so that the cache has a newer put than the one the held archive item waits for.
let output = 0x[00 68 65 6c 6c 6f] | timeout 10 tg object put --no-tokens --bytes --kind blob | complete
success $output

# The archive item of the older put must finish without an upload, so that the archive item of the newer put uploads the object.
checkpoint unwatch $checkpoint
let requests = http get --max-time 30sec $'($archive.url)/requests?count=1'
assert equal ($requests | length) 1 'the newer put must be uploaded'
assert equal $requests.0.path $'/($id)'

http post $'($archive.url)/respond' '200' | ignore
server stop $local
job kill $archive.job
