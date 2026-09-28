use ../lib/test.nu *

# A checked-in output allows completion and Finish before control returns.

let local = server spawn --config {
	advanced: { checkpoints: true },
}

for checkpoint in [runner.process.control.connect process.control.output] {
	let control_watch = tg --url $local.url checkpoint watch $checkpoint | from json | get watch
	let stored_watch = tg --url $local.url checkpoint watch runner.process.output.stored | from json | get watch
	let finished_watch = tg --url $local.url checkpoint watch runner.process.finished | from json | get watch
	let sent_watch = tg --url $local.url checkpoint watch runner.process.control.finish.sent | from json | get watch
	let received_watch = tg --url $local.url checkpoint watch process.control.finish | from json | get watch

	let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf \"%s\" \"$1\" > \"$TANGRAM_OUTPUT\""), "executable": true })'
	let file = tg --url $local.url put $artifact | str trim
	let build = job spawn {
		let job_id = job id
		let output = tg --url $local.url build $file --arg-string $checkpoint | complete
		$output | job send --tag $job_id 0
	}

	# Store the output while the connection or initial indexing is held.
	success (timeout 30s tg --url $local.url checkpoint wait $checkpoint $control_watch 0 | complete) "should reach $checkpoint"
	success (timeout 30s tg --url $local.url checkpoint wait runner.process.output.stored $stored_watch 0 | complete) "output collection should not wait for control"
	tg --url $local.url checkpoint unwatch runner.process.output.stored $stored_watch
	success (timeout 30s tg --url $local.url checkpoint wait runner.process.finished $finished_watch 0 | complete) "completion should not wait for the control connection or indexing"
	tg --url $local.url checkpoint unwatch runner.process.finished $finished_watch
	success (timeout 30s tg --url $local.url checkpoint wait runner.process.control.finish.sent $sent_watch 0 | complete) "Finish should be queued before control returns"
	tg --url $local.url checkpoint unwatch runner.process.control.finish.sent $sent_watch
	tg --url $local.url checkpoint unwatch $checkpoint $control_watch

	success (timeout 30s tg --url $local.url checkpoint wait process.control.finish $received_watch 0 | complete) "the runner should send Finish"
	tg --url $local.url checkpoint unwatch process.control.finish $received_watch
	let output = job recv --tag $build --timeout 30sec
	success $output "the build should complete after control is released"
	let file = $output.stdout | str trim
	assert equal (tg --url $local.url read $file) $checkpoint
}
