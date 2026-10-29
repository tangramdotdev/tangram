use ../../util/test.nu *
use ../../util/archive.nu *

# Graceful shutdown waits for a single-server blob batch to finish archiving.
skip_test --cloud
let archive = spawn_archive
let local = server spawn --cloud --config {
	archive: $archive.config,
	checkouts: false,
	object: { put_timeout: 5 },
	roles: [api indexer],
}
let output = 'hello' | timeout 10 tg write --no-tokens | complete
success $output
let requests = http get --max-time 30sec $'($archive.url)/requests?count=1'
assert equal ($requests | length) 1 'the upload must start'

let stop = job spawn {
	let id = job id
	server stop $local
	true | job send --tag $id 0
}
let stopped = try { job recv --tag $stop --timeout 1sec } catch { null }
assert ($stopped == null) 'shutdown must wait for the pending upload'
http post $'($archive.url)/respond' '200' | ignore
let stopped = job recv --tag $stop --timeout 10sec
assert $stopped
assert ($local.exit | path exists)
job kill $archive.job
