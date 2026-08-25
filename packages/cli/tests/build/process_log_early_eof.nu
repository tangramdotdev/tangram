use ../../test.nu *

# Reproduces a race where sync observes a process before its log is compacted.

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

# Read from remote blob should not fail with early eof.
let output = tg --url $remote.url process log $id | complete
success $output "Log read failed"
assert equal ($output.stdout | lines | length) 9900 "The remote log should contain every line"
