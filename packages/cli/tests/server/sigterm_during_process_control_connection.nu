use ../../test.nu *

# SIGTERM terminates a process that starts before its process control stream connects.

let server = server spawn --config {
	advanced: { checkpoints: true },
}

let control_watch = (
	tg checkpoint watch runner.process.control.connect
	| from json
	| get watch
)
let start_watch = (
	tg checkpoint watch runner.process.start
	| from json
	| get watch
)

let path = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(60);
		};
	'
}
let build = job spawn {
	let job_id = job id
	let output = tg build $path | complete
	$output | job send --tag $job_id 0
}

# Hold process control before it connects, then allow the process to start.
let output = timeout 10s tg checkpoint wait runner.process.control.connect $control_watch 0 | complete
success $output "process control must reach the connection checkpoint"
let output = timeout 10s tg checkpoint wait runner.process.start $start_watch 0 | complete
success $output "the process must reach the start checkpoint"
tg checkpoint continue runner.process.start $start_watch 0
tg checkpoint unwatch runner.process.start $start_watch
sleep 1sec

# Send SIGTERM to the server while process control remains blocked.
let pid = open ($server.directory | path join 'lock') | into int
kill --signal 15 $pid

# The server and build request must exit without process control connecting.
wait_until --timeout 20sec { ps | where pid == $pid | is-empty } "the server must exit without waiting for process control"
let output = try { job recv --tag $build --timeout 10sec } catch { null }
assert ($output != null) "the build request must exit with the server"
