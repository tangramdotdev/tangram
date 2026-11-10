use ../../util/test.nu *

# Running and starting processes report an OOM kill as a failure.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let cgroup_parent = container_cgroup_parent [cpu memory]
let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { backend: { kind: builtin } },
}
let memory = 134_217_728
let sandbox = tg sandbox create --no-tokens --memory $memory | referent node

# Hold a process before it is spawned in the sandbox.
let start_watch = tg checkpoint watch runner.process.start | from json | get watch
let shutdown_watch = tg checkpoint watch runner.sandbox.shutdown | from json | get watch
let starting = tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'sleep 1000' | referent node
tg checkpoint wait --timeout 30s runner.process.start $start_watch 0 | ignore

# Exceed the sandbox's memory limit with a second process.
let script = 'value=x; while :; do value="$value$value"; done'
let running = tg spawn --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c $script | referent node
tg checkpoint wait --timeout 30s runner.process.start $start_watch 1 | ignore
tg checkpoint continue runner.process.start $start_watch 1

# Release the starting process after the sandbox's server has shut down.
tg checkpoint wait --timeout 30s runner.sandbox.shutdown $shutdown_watch 0 | ignore
tg checkpoint continue runner.sandbox.shutdown $shutdown_watch 0
tg checkpoint unwatch runner.sandbox.shutdown $shutdown_watch
tg checkpoint continue runner.process.start $start_watch 0
tg checkpoint unwatch runner.process.start $start_watch

# Both processes report a failure without classifying it as a cancellation.
for process in [$running $starting] {
	let output = timeout 30s tg wait $process | complete
	success $output 'the process should finish after an OOM kill'
	let outcome = $output.stdout | from json
	assert equal $outcome.exit 1
	let error = tg get --depth inf --no-tokens $outcome.error
	assert not ($error | str contains '"code":"cancellation"') 'an OOM kill should not report a cancellation'
	if $process == $running {
		assert ($error | str contains 'the sandbox exceeded its memory max') $error
		assert ($error | str contains $'"memory_max":"($memory)"') $error
	}
}
