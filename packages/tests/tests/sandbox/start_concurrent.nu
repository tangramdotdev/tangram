use ../../util/test.nu *

# Two starts of a stopped sandbox create one boot, because only one of them reserves it.

let local = server spawn --config {
	advanced: { checkpoints: true },
}
let socket = $local.url | str replace 'http+unix://' '' | url decode

# Stop a sandbox.
let sandbox = tg sandbox create --wait --no-tokens | referent node
tg sandbox stop --wait $sandbox
assert equal (tg sandbox get $sandbox | from json | get data.boot? | default 0) 0

# Hold two starts after each has read that the sandbox is stopped, and then release them together.
let reserve = checkpoint watch sandbox.start.reserve --params { sandbox: $sandbox }
let starts = [0 1] | each { |_|
	job spawn {
		let job_id = job id
		let output = http post --allow-errors --full --max-time 30sec --unix-socket $socket --content-type application/json $'http://localhost/sandboxes/($sandbox)/start' '{"wait":true}'
		$output.status | job send --tag $job_id 0
	}
}
checkpoint wait $reserve 0 | ignore
checkpoint wait $reserve 1 | ignore
checkpoint unwatch $reserve

# One start creates the next boot, and the other finds the sandbox already starting.
let statuses = $starts | each { |start| job recv --tag $start --timeout 30sec }
assert equal ($statuses | sort) [200 409]
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.boot 1
assert equal $data.status started
sleep 1sec
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.boot 1
assert equal $data.status started
tg sandbox destroy $sandbox
