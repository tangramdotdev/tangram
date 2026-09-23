use ../lib/test.nu *

# Sync waits for background compaction and transfers the compacted log.

for mode in [--eager --lazy] {
	let remote = server spawn --cloud --name remote
	let local = server spawn --name local --config {
		advanced: { checkpoints: true },
	}
	tg remote put default $remote.url
	let watch = tg checkpoint watch process.log.compact.read | from json | get watch
	let path = artifact {
		tangram.ts: 'export default function () { console.log("stdout"); console.error("stderr"); }'
	}
	let process = tg build --detach $path | str trim
	timeout 10s tg wait $process
	let hit = timeout 10s tg checkpoint wait process.log.compact.read $watch 0 | from json
	assert equal $hit.params.process $process

	# Sync must wait while the queued compactor is held before reading the cache.
	let push_job = job spawn {
		let job_id = job id
		let output = tg --url $local.url push $process --process-logs $mode | complete
		$output | job send --tag $job_id 0
	}
	let premature = try { job recv --tag $push_job --timeout 1sec } catch { null }
	assert equal $premature null "sync must wait for log compaction"

	# Finish background compaction, including deletion of the cached log entries.
	tg checkpoint continue process.log.compact.read $watch 0
	timeout 10s tg index
	let log = tg get $process | from json | get log
	let output = tg log $process --no-timeout | complete
	success $output
	assert equal $output.stdout "stdout\n"
	assert equal $output.stderr "stderr\n"

	# Verify that sync transfers the compacted log after the cache is empty.
	tg checkpoint unwatch process.log.compact.read $watch
	let output = job recv --tag $push_job --timeout 10sec
	success $output
	assert equal (tg get $process | from json | get log) $log
	assert equal (tg --url $remote.url get $process | from json | get log) $log
	let output = tg --url $remote.url log $process --no-timeout | complete
	success $output
	assert equal $output.stdout "stdout\n"
	assert equal $output.stderr "stderr\n"
}
