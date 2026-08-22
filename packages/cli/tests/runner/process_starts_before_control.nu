use ../../test.nu *

# A runner starts a locally available command without waiting for process control to connect.

let server = spawn --config {
	advanced: { checkpoints: true },
}

let control_watch = (
	tg --url $server.url checkpoint watch runner.process.control.connect
	| from json
	| get watch
)
let start_watch = (
	tg --url $server.url checkpoint watch runner.process.start
	| from json
	| get watch
)
let state_watch = (
	tg --url $server.url checkpoint watch runner.process.state.inserted
	| from json
	| get watch
)

let path = artifact {
	tangram.ts: 'export default () => 42',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $server.url build $path | complete
	$output | job send --tag $job_id 0
}

# Observe the stored state, then hold process control before it connects. The process must still reach the sandbox start.
let output = timeout 5s tg --url $server.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete
success $output "the process state should be stored before process control connects"
tg --url $server.url checkpoint continue runner.process.state.inserted $state_watch 0
tg --url $server.url checkpoint unwatch runner.process.state.inserted $state_watch

let output = timeout 5s tg --url $server.url checkpoint wait runner.process.control.connect $control_watch 0 | complete
success $output "process control should reach the connection checkpoint"
let output = timeout 5s tg --url $server.url checkpoint wait runner.process.start $start_watch 0 | complete
success $output "the process should start before process control connects"

tg --url $server.url checkpoint continue runner.process.start $start_watch 0
tg --url $server.url checkpoint unwatch runner.process.start $start_watch
tg --url $server.url checkpoint continue runner.process.control.connect $control_watch 0
tg --url $server.url checkpoint unwatch runner.process.control.connect $control_watch

let output = job recv --tag $build --timeout 10sec
if $output == null {
	error make { msg: "the build did not complete after process control connected" }
}
success $output "the build should complete after process control connects"
