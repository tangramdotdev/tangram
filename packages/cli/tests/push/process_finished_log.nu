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
		tg checkpoint wait --timeout 10s runner.process.log.finish $log_watch 0 | ignore
		tg checkpoint wait --timeout 10s runner.process.output.stored $output_watch 0 | ignore
		let held = tg get $process | from json
		assert equal $held.log? null
		assert not equal $held.status finished "Finish must wait for the log blob"
		tg checkpoint unwatch runner.process.output.stored $output_watch
		tg checkpoint unwatch runner.process.log.finish $log_watch
		success (timeout 10s tg wait $process | complete)
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
