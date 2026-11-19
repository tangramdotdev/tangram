use ../../util/test.nu *

# The sandbox that a spawn creates on this server's runner is listed as starting before its process starts, and is listed once the spawn returns.

let server = server spawn --config { advanced: { checkpoints: true } }

# Hold the spawn when its sandbox's control connects, before the process starts.
let connect = checkpoint watch sandbox.control.connect
let spawn = job spawn {
	let id = job id
	tg spawn --no-tokens --verbose --sandbox --executable /bin/sh -- -c 'exit 0' | complete | job send --tag $id 0
}
let sandbox = checkpoint wait $connect 0 | get params.sandbox
tg index
let listed = tg sandbox list --status starting --expand | from json | where data.id == $sandbox
assert equal ($listed | length) 1 'the sandbox should be listed before the process starts'
assert equal ($listed | first | get data.status) starting

# Let the spawn return, and the sandbox is listed with its process.
checkpoint unwatch $connect
let output = job recv --tag $spawn --timeout 30sec
success $output 'the spawn should succeed'
let process = $output.stdout | from json | get process
tg index
assert equal (tg process get $process | from json | get sandbox) $sandbox
assert equal (tg sandbox list --any-status | from json | where node == $sandbox | length) 1 'the sandbox should be listed after the spawn returns'
