use ../../test.nu *
use ../lib/archive.nu *

# Graceful shutdown waits for a single-process blob batch to finish archiving.
skip_if_no_cloud
let archive = spawn_archive
let server = server spawn --cloud --config {
	advanced: { single_directory: false, single_process: true },
	archive: $archive.config,
	object: { put_timeout: 5 },
	roles: [api indexer],
}
let output = 'hello' | timeout 10 tg write | complete
success $output
wait_until { http get $'($archive.url)/requests' | length | $in == 1 } 'the upload must start'

let stop = job spawn {
	let id = job id
	server stop $server
	true | job send --tag $id 0
}
let stopped = try { job recv --tag $stop --timeout 1sec } catch { null }
assert ($stopped == null) 'shutdown must wait for the pending upload'
http post $'($archive.url)/respond' '200' | ignore
let stopped = job recv --tag $stop --timeout 10sec
assert $stopped
assert ($server.exit | path exists)
job kill $archive.job
