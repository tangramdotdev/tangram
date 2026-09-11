use ../../test.nu *

# A sync that reads the log cache after background compaction must preserve the compacted log.

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

	# Both callers must observe an uncompacted log before either reads the cache.
	let push_job = job spawn {
		let job_id = job id
		let output = tg --url $local.url push $process --process-logs $mode | complete
		$output | job send --tag $job_id 0
	}
	let hit = timeout 10s tg checkpoint wait process.log.compact.read $watch 1 | from json
	assert equal $hit.params.process $process

	# Finish background compaction, including deletion of the cached log entries.
	tg checkpoint continue process.log.compact.read $watch 0
	timeout 10s tg index
	let log = tg get $process | from json | get log
	let output = tg log $process --no-timeout | complete
	success $output
	assert equal $output.stdout "stdout\n"
	assert equal $output.stderr "stderr\n"

	# Resume sync after the cache is empty and verify that it retains the same log.
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
