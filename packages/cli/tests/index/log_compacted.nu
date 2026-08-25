use ../../test.nu *

# A finished process wakes log compaction without waiting for the fallback interval.

let local = server spawn --name local --config {
	indexer: { log_compaction: { wakeup_interval: 3600 } },
}

let path = artifact {
	tangram.ts: r#'
		export default function () {}
	'#
}
let id = tg build --detach $path | str trim
tg wait $id

timeout 10 tg index

let process = tg get $id | from json
let log_id = $process.log
let log = tg get $log_id --blobs
snapshot --name log $log 'tg.blob("\u0000\u000b\n\u0003\u0000\b\u0000\u0001\b\u0000\u0002\b\u0000")'
