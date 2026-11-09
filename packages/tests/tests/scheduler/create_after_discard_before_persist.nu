use ../../util/test.nu *

# A sandbox create that no runner accepts, because no runner can fit the sandbox, replies once its index write finishes, and does not hang.

let server = server spawn --config {
	advanced: { checkpoints: true },
	roles: [api indexer scheduler],
	scheduler: { unplaceable_timeout: 0.25 },
}

# Hold the scheduler's index write, and create a sandbox.
let persist = checkpoint watch scheduler.sandbox.persist
let url = $server.url
let create = job spawn {
	let id = job id
	tg --url $url sandbox create --no-tokens | complete | job send --tag $id 0
}
checkpoint wait $persist | ignore

# The create must not reply while the index write is held, although the scheduler discards the sandbox.
let pending = try {
	job recv --tag $create --timeout 1sec | ignore
	false
} catch {
	true
}
assert $pending 'the create should wait for the index write after the sandbox is discarded'

# The create replies once the index write is released, and the sandbox records the failure.
checkpoint unwatch $persist
let output = job recv --tag $create --timeout 30sec
success $output 'the create should reply once the index write finishes'
let sandbox = $output.stdout | str trim
success (tg --url $url sandbox wait $sandbox | complete) 'the sandbox should end'
let data = tg --url $url sandbox get $sandbox | from json
assert equal $data.data.status 'destroyed'
assert ($data.data.error.message | str contains 'no runners available')
