use ../../test.nu *
use ../lib/archive.nu *

# Single-process indexing waits for background archiving to succeed, including retries after an upload failure.
skip_if_no_cloud
let archive = spawn_archive
let server = server spawn --cloud --config {
	advanced: { checkpoints: true, single_directory: false, single_process: true },
	archive: $archive.config,
	object: { put_timeout: 5 },
	roles: [api indexer],
}
assert ($server.config.indexer?.id? == null)

# The put must return while the archive holds its upload response.
let output = 0x[00 68 65 6c 6c 6f] | timeout 10 tg object put --bytes --kind blob | complete
success $output
let id = $output.stdout | str trim
wait_until { http get $'($archive.url)/requests' | length | $in == 1 } 'the upload must start'

let watch = tg checkpoint watch indexer.request.wait | from json | get watch
let request = job spawn {
	let id = job id
	tg index | complete | job send --tag $id 0
}
tg checkpoint wait indexer.request.wait $watch 0 | ignore
tg checkpoint unwatch indexer.request.wait $watch
let output = try { job recv --tag $request --timeout 200ms } catch { null }
assert ($output == null) 'indexing must wait for the upload'

# A failed upload must retry the same object and put, and keep indexing pending.
http post $'($archive.url)/respond' '503' | ignore
wait_until { http get $'($archive.url)/requests' | length | $in == 2 } 'the upload must retry'
let requests = http get $'($archive.url)/requests'
assert equal $requests.0 $requests.1
assert equal $requests.0.path $'/($id)'
let output = try { job recv --tag $request --timeout 200ms } catch { null }
assert ($output == null) 'indexing must wait for the retried upload'

http post $'($archive.url)/respond' '200' | ignore
let output = job recv --tag $request --timeout 10sec
success $output
server stop $server
job kill $archive.job
