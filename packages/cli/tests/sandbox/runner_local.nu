use ../../test.nu *

# Sandbox status comes from the runner even while its destroy index update is delayed.

let server = server spawn --config {
	advanced: { checkpoints: true },
	sandbox: { status_wakeup_interval: 3600.0 },
}
let sandbox = tg sandbox create | str trim
let socket = $server.url | str replace 'http+unix://' '' | url decode
let query = { location: 'local(hint)' } | url build-query
let output = http get --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)?($query)'
assert equal $output.data.status started
let wait_job = job spawn {
	let job_id = job id
	http get --max-time 30sec --raw --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?($query)'
	| lines
	| where { $in in ['data: "started"' 'data: "destroyed"' 'event: end'] }
	| each { $in | job send --tag $job_id 0 }
	| ignore
}
assert equal (job recv --tag $wait_job --timeout 10sec) 'data: "started"'
let destroy_watch = tg checkpoint watch sandbox.control.destroy | from json | get watch
tg sandbox destroy $sandbox
timeout 30s tg checkpoint wait sandbox.control.destroy $destroy_watch 0 | ignore
assert equal (job recv --tag $wait_job --timeout 10sec) 'data: "destroyed"'
assert equal (job recv --tag $wait_job --timeout 10sec) 'event: end'

# New wait requests can still read a destroyed sandbox after completion is published.
tg checkpoint continue sandbox.control.destroy $destroy_watch 0
tg checkpoint unwatch sandbox.control.destroy $destroy_watch
assert equal (timeout 10s tg sandbox wait $sandbox | from json) destroyed
