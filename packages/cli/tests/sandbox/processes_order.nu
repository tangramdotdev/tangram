use ../lib/test.nu *

# Arrival order, rather than ID order, defines positions and survives final indexing.
let server = server spawn --config {
	advanced: { checkpoints: true },
	runner: { sandbox_state_ttl: 60 },
}
let sandbox = tg sandbox create | str trim
let socket = $server.url | str replace 'http+unix://' '' | url decode
let path = artifact { tangram.ts: 'export default () => "done";' }
let watch = tg checkpoint watch runner.process.state.insert | from json | get watch
let first = job spawn {
	let job_id = job id
	let process = tg spawn $'--sandbox=($sandbox)' $path | str trim
	$process | job send --tag $job_id 0
}
timeout 10s tg checkpoint wait runner.process.state.insert $watch 0 | ignore
let second = job spawn {
	let job_id = job id
	let process = tg spawn $'--sandbox=($sandbox)' $path | str trim
	$process | job send --tag $job_id 0
}
timeout 10s tg checkpoint wait runner.process.state.insert $watch 1 | ignore
tg checkpoint continue runner.process.state.insert $watch 1
let second = job recv --tag $second --timeout 10sec
let reader = job spawn {
	let job_id = job id
	http get --raw --max-time 20sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?source=runner&size=1&length=2'
	| lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json | job send --tag $job_id 0 } | ignore
}
assert equal (job recv --tag $reader --timeout 10sec) { data: [$second], position: 0 }
tg checkpoint continue runner.process.state.insert $watch 0
let first = job recv --tag $first --timeout 10sec
assert equal (job recv --tag $reader --timeout 10sec) { data: [$first], position: 1 }
tg checkpoint unwatch runner.process.state.insert $watch
tg wait --source=index $first | ignore
tg wait --source=index $second | ignore
tg sandbox destroy $sandbox
tg sandbox wait --source=index $sandbox | ignore
let output = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?source=index&size=1'
let chunks = $output | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json }
assert equal ($chunks | get data | flatten) [$second $first]
assert equal ($chunks | get position) [0 1]
