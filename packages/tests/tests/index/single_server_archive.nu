use ../../lib/test.nu *
use ../../lib/archive.nu *

# Single-server indexing completes independently of background archiving, including retries after an upload failure.
skip_test --cloud
let archive = spawn_archive
let local = server spawn --cloud --config {
	archive: $archive.config,
	object: { put_timeout: 5 },
	roles: [api indexer],
}
assert ($local.config.indexer?.id? == null)

# The put must return while the archive holds its upload response.
let output = 0x[00 68 65 6c 6c 6f] | timeout 10 tg object put --no-tokens --bytes --kind blob | complete
success $output
let id = $output.stdout | referent node
let requests = http get --max-time 30sec $'($archive.url)/requests?count=1'
assert equal ($requests | length) 1 'the upload must start'

# Indexing must return while the upload is pending.
let output = timeout 10 tg index | complete
success $output

# A failed upload must retry the same object and put without blocking indexing.
http post $'($archive.url)/respond' '503' | ignore
let requests = http get --max-time 30sec $'($archive.url)/requests?count=2'
assert equal ($requests | length) 2 'the upload must retry'
assert equal $requests.0 $requests.1
assert equal $requests.0.path $'/($id)'
let output = timeout 10 tg index | complete
success $output

http post $'($archive.url)/respond' '200' | ignore
server stop $local
job kill $archive.job
