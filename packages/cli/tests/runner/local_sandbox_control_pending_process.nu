use ../../test.nu *

# A pending process connection must not block sandbox destruction or subsequent control requests.
let server = server spawn --config { advanced: { checkpoints: true } }
let sandbox = tg sandbox create | str trim
let watch = tg checkpoint watch runner.process.control.connect | from json | get watch
let path = artifact { tangram.ts: 'export default () => tg.sleep(60);' }
let spawn = job spawn {
	let job_id = job id
	let output = tg run $'--sandbox=($sandbox)' $path | complete
	$output | job send --tag $job_id 0
}
timeout 30s tg checkpoint wait runner.process.control.connect $watch 0 | ignore
success (timeout 10s tg sandbox destroy $sandbox | complete) "destruction must not wait for a pending process connection"
let output = timeout 10s tg sandbox destroy $sandbox | complete
failure $output
assert ($output.exit_code != 124) "the repeated destroy request must also remain responsive"
tg checkpoint unwatch runner.process.control.connect $watch
failure (job recv --tag $spawn --timeout 30sec)
assert equal (timeout 30s tg sandbox wait $sandbox | from json) destroyed
