use ../lib/test.nu *

# Finish waits for log finalization while output checkin proceeds concurrently.
for mode in [--eager --lazy] {
	for empty in [false true] {
		let remote = server spawn --cloud --name remote
		let local = server spawn --name local --config { advanced: { checkpoints: true } }
		tg remote put default $remote.url
		let log_watch = tg checkpoint watch runner.process.log.finish | from json | get watch
		let output_watch = tg checkpoint watch runner.process.output.stored | from json | get watch
		let source = if $empty { 'export default function () {}' } else { 'export default function () { console.log("stdout"); console.error("stderr"); return "output"; }' }
		let path = artifact { tangram.ts: $source }
		let process = tg build --no-tokens --detach $path | referent node
		timeout 10s tg checkpoint wait runner.process.log.finish $log_watch 0 | ignore
		timeout 10s tg checkpoint wait runner.process.output.stored $output_watch 0 | ignore
		assert equal (tg get $process | from json | get log?) null
		let wait_job = job spawn {
			let job_id = job id
			let output = tg --url $local.url wait $process | complete
			$output | job send --tag $job_id 0
		}
		let premature = try { job recv --tag $wait_job --timeout 1sec } catch { null }
		assert equal $premature null "Finish must wait for the log blob"
		tg checkpoint unwatch runner.process.output.stored $output_watch
		tg checkpoint unwatch runner.process.log.finish $log_watch
		success (job recv --tag $wait_job --timeout 10sec)
		let data = tg get --no-tokens $process | from json
		assert ($data.log? | is-not-empty)
		tg push --process-log-objects $mode $process
		let remote_data = tg --url $remote.url get --no-tokens $process | from json
		assert equal $data $remote_data
		let log = tg --url $remote.url log $process --no-timeout | complete
		success $log
		assert equal $log.stdout (if $empty { "" } else { "stdout\n" })
		assert equal $log.stderr (if $empty { "" } else { "stderr\n" })
	}
}
