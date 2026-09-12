use ../../test.nu *

# Verify that sync waits for the live log to end, compacts it, and transfers every line.

let local = server spawn --name local --config { indexer: { log_compaction: false } }
let remote = server spawn --name remote

let path = artifact {
	tangram.ts: '
		export default function () {
			for (let i = 0; i < 9900; i++) {
				console.log(`Line ${i.toString().padStart(4, "0")}: ${"x".repeat(200)}`);
			}
		}
	'
}

let id = tg --url $local.url build --detach $path | str trim
tg --url $local.url wait $id
assert ((tg --url $local.url get $id | from json | get log?) == null) "The source log should remain live before the push"
tg --url $local.url remote put default $remote.url | complete
tg --url $local.url push --process-logs $id

let log = tg --url $remote.url get $id | from json | get log?
assert ($log != null) "sync should compact and transfer the log"
assert equal $log (tg --url $local.url get $id | from json | get log)
let output = tg --url $remote.url log $id --no-timeout | complete
success $output
assert equal ($output.stdout | lines | length) 9900 "the transferred log should contain every line"
