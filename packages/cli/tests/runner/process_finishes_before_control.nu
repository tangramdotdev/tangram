use ../../test.nu *

# A process that exits before process control connects delivers its finish in the connect request instead of a separate finish request.

let server = server spawn --config {
	advanced: { checkpoints: true },
}

let control_watch = (
	tg --url $server.url checkpoint watch runner.process.control.connect
	| from json
	| get watch
)
let exit_watch = (
	tg --url $server.url checkpoint watch runner.process.exit
	| from json
	| get watch
)
let connect_finish_watch = (
	tg --url $server.url checkpoint watch process.control.connect.finish
	| from json
	| get watch
)
let finish_request_watch = (
	tg --url $server.url checkpoint watch runner.process.control.finish.request
	| from json
	| get watch
)

let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf \"%s\" \"$1\" > \"$TANGRAM_OUTPUT\""), "executable": true })'
let file = tg --url $server.url put $artifact | str trim
let build = job spawn {
	let job_id = job id
	let output = tg --url $server.url build $file --arg-string hello | complete
	$output | job send --tag $job_id 0
}

# Hold process control before it connects and let the process run to completion.
let output = timeout 5s tg --url $server.url checkpoint wait runner.process.control.connect $control_watch 0 | complete
success $output "process control should reach the connection checkpoint"
let output = timeout 5s tg --url $server.url checkpoint wait runner.process.exit $exit_watch 0 | complete
success $output "the process should exit before process control connects"
tg --url $server.url checkpoint continue runner.process.exit $exit_watch 0
tg --url $server.url checkpoint unwatch runner.process.exit $exit_watch
sleep 100ms
tg --url $server.url checkpoint continue runner.process.control.connect $control_watch 0
tg --url $server.url checkpoint unwatch runner.process.control.connect $control_watch

# The server records the finish while handling the connect request.
let output = timeout 5s tg --url $server.url checkpoint wait process.control.connect.finish $connect_finish_watch 0 | complete
success $output "the connect request should carry the finished process"
tg --url $server.url checkpoint continue process.control.connect.finish $connect_finish_watch 0
tg --url $server.url checkpoint unwatch process.control.connect.finish $connect_finish_watch

let output = try { job recv --tag $build --timeout 10sec } catch { null }
if $output == null {
	error make { msg: "the build did not complete after process control connected" }
}
success $output "the build should complete after process control connects"
assert ($output.stdout | str contains 'fil_')

# The runner never sends a separate finish request.
let output = timeout 2s tg --url $server.url checkpoint wait runner.process.control.finish.request $finish_request_watch 0 | complete
failure $output "the runner should not send a finish request for a process that finished before connecting"
tg --url $server.url checkpoint unwatch runner.process.control.finish.request $finish_request_watch
