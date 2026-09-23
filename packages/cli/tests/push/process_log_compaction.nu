use ../lib/test.nu *

# Sync waits for the queued log compaction before sending the process.

for mode in [--eager --lazy] {
	for empty in [false true] {
		let remote = server spawn --cloud --name remote
		let local = server spawn --name local --config {
			advanced: { checkpoints: true },
		}
		tg remote put default $remote.url

		let watch = tg checkpoint watch process.control.log.end | from json | get watch
		let source = if $empty {
			'export default function () {}'
		} else {
			'export default function () { console.log("stdout"); console.error("stderr"); }'
		}
		let path = artifact { tangram.ts: $source }
		let process = tg build --detach $path | str trim
		timeout 10s tg checkpoint wait process.control.log.end $watch 0 | ignore
		timeout 10s tg wait $process
		assert equal (tg get $process | from json | get log?) null

		# The process is finished, but sync must wait for the log compactor to see EOF.
		let push_job = job spawn {
			let job_id = job id
			let output = tg --url $local.url push $process --process-logs $mode | complete
			$output | job send --tag $job_id 0
		}
		let premature = try { job recv --tag $push_job --timeout 1sec } catch { null }
		assert equal $premature null "sync must wait for the log to end"
		assert equal (tg get $process | from json | get log?) null "the log must not compact before EOF"

		tg checkpoint unwatch process.control.log.end $watch
		let output = job recv --tag $push_job --timeout 10sec
		success $output

		# The queued compactor must finish before sync transfers the complete log.
		let data = tg get $process | from json
		assert ($data.log? | is-not-empty) "the log must be compacted"
		let remote_data = tg --url $remote.url get $process | from json
		assert equal $data $remote_data
		let log = tg --url $remote.url log $process --no-timeout | complete
		success $log
		assert equal $log.stdout (if $empty { "" } else { "stdout\n" })
		assert equal $log.stderr (if $empty { "" } else { "stderr\n" })
	}
}

# A disabled compactor cannot satisfy a push that requests process logs.
let remote = server spawn --cloud --name remote
let local = server spawn --name local --config { indexer: { log_compaction: false } }
tg remote put default $remote.url
let path = artifact { tangram.ts: 'export default function () { console.log("stdout"); }' }
let process = tg build --detach $path | str trim
tg wait $process | ignore
failure (tg --url $local.url push $process --process-logs --eager | complete) "push must report an uncompacted log"
