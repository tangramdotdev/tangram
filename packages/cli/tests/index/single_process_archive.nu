use ../lib/test.nu *
use ../lib/archive.nu *

# Single-process indexing completes independently of background archiving, including retries after an upload failure.
skip_if_no_cloud
let archive = spawn_archive
let local = server spawn --cloud --config {
	advanced: { single_directory: false, single_process: true },
	archive: $archive.config,
	object: { put_timeout: 5 },
	roles: [api indexer],
}
assert ($local.config.indexer?.id? == null)

# The put must return while the archive holds its upload response.
let output = 0x[00 68 65 6c 6c 6f] | timeout 10 tg object put --bytes --kind blob | complete
success $output
let id = $output.stdout | str trim
wait_until { http get $'($archive.url)/requests' | length | $in == 1 } 'the upload must start'

# Indexing must return while the upload is pending.
let output = timeout 10 tg index | complete
success $output

# A failed upload must retry the same object and put without blocking indexing.
http post $'($archive.url)/respond' '503' | ignore
wait_until { http get $'($archive.url)/requests' | length | $in == 2 } 'the upload must retry'
let requests = http get $'($archive.url)/requests'
assert equal $requests.0 $requests.1
assert equal $requests.0.path $'/($id)'
let output = timeout 10 tg index | complete
success $output

http post $'($archive.url)/respond' '200' | ignore
server stop $local
job kill $archive.job
