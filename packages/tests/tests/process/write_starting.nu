use ../../util/test.nu *

# A signal and a tty resize sent to a process that is still starting reach the process once it starts, on the process's status change rather than after the sender's backoff.

let local = server spawn --config {
	control: { request_retry: { backoff: 60, jitter: 0, max_delay: 60 } },
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let socket = $local.url | str replace 'http+unix://' '' | url decode
let path = artifact {
	tangram.ts: '
		export function spin() {
			return tg.command({
				args: ["-ec", "while :; do :; done"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let mount = artifact {}
let spin = tg build $'($path)#spin' | str trim

# Fill the runner, so that the process waits in the queue, and signal it while it is starting.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let spawned = tg process spawn --no-tokens --verbose --sandbox=true --mount $"($mount):/target" $spin | from json
assert equal (tg get $spawned.process | from json | get status) starting
let signal = job spawn {
	let id = job id
	timeout 30s tg signal --signal KILL $spawned.process | complete | job send --tag $id 0
}
sleep 1sec
tg sandbox destroy $sandbox
let output = job recv --tag $signal --timeout 30sec
success $output "the signal should reach the process once it starts"
let outcome = timeout 30s tg wait $spawned.process | from json
assert ($outcome.exit != 0) "the signal should kill the process"

# Fill the runner again, and resize the tty of a process while it is starting.
let sandbox = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let spawned = tg process spawn --no-tokens --verbose --sandbox=true --mount $"($mount):/target" --tty=24,80 $spin | from json
assert equal (tg get $spawned.process | from json | get status) starting
let body = { size: { cols: 100, rows: 40 } } | to json --raw
let resize = job spawn {
	let id = job id
	let status = http put --allow-errors --full --max-time 30sec --unix-socket $socket --content-type application/json $'http://localhost/processes/($spawned.process)/tty/size' $body | get status
	$status | job send --tag $id 0
}
sleep 1sec
tg sandbox destroy $sandbox
let status = job recv --tag $resize --timeout 30sec
assert equal $status 200 "the resize should reach the process once it starts"
tg signal --signal KILL $spawned.process
tg wait $spawned.process | ignore
