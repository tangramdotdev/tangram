use ../../util/test.nu *

# A destroy that is concurrent with a stop destroys the sandbox.

let local = server spawn
let url = $local.url

let sandbox = tg sandbox create --wait --no-tokens | referent node

# Stop and destroy the sandbox at once.
let stop = job spawn {
	let id = job id
	timeout 30s tg --url $url sandbox stop --wait $sandbox | complete | job send --tag $id 0
}
let destroy = job spawn {
	let id = job id
	timeout 30s tg --url $url sandbox destroy --wait $sandbox | complete | job send --tag $id 0
}
job recv --tag $stop --timeout 40sec | ignore
success (job recv --tag $destroy --timeout 40sec) "the destroy should finish"
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed "the sandbox should be destroyed"
