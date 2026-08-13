use ../../test.nu *

# Waiting for a sandbox blocks until it is destroyed and is idempotent.

let server = spawn

let sandbox = tg sandbox create | str trim
let wait = job spawn {
	let job_id = job id
	let output = tg wait $sandbox | complete
	$output | job send --tag $job_id 0
}

let early = try {
	job recv --tag $wait --timeout 250ms
} catch {
	null
}
assert ($early == null) "waiting for the sandbox should block"

tg sandbox destroy $sandbox

let output = job recv --tag $wait --timeout 10sec
success $output
assert equal ($output.stdout | from json) destroyed "waiting for the sandbox should return its terminal status"

let output = tg sandbox wait $sandbox | from json
assert equal $output destroyed "waiting for a destroyed sandbox should return immediately"

# A sandbox can be waited on through a remote reference.
let origin = spawn --name origin
let local = spawn --name local
tg --url $local.url remote put origin $origin.url
let sandbox = tg --url $origin.url sandbox create | str trim
let wait = job spawn {
	let job_id = job id
	let output = tg --url $local.url wait $'($sandbox)?location=remote:origin' | complete
	$output | job send --tag $job_id 0
}
tg --url $origin.url sandbox destroy $sandbox
let output = job recv --tag $wait --timeout 10sec
success $output
assert equal ($output.stdout | from json) destroyed "waiting through a remote reference should succeed"

# Waiting for a missing sandbox fails.
let missing = tg sandbox wait sbx_010000000000000000000000000000000000000000000000000000 | complete
failure $missing
assert ($missing.stderr | str contains "failed to wait for the sandbox")
assert ($missing.stderr | str contains "failed to find the sandbox")
