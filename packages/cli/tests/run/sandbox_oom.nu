use ../lib/test.nu *

# A process whose sandbox is killed by the out-of-memory killer reports the OOM kill.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let cgroup_parent = container_cgroup_parent [cpu memory]
let local = server spawn --config {
	runner: { backend: { kind: builtin } },
}
let memory = 134_217_728
let script = 'value=x; while :; do value="$value$value"; done'
let process = tg spawn --sandbox --memory $memory --swap 0 --verbose --executable /bin/sh -- -c $script | from json | get process
let output = timeout 30s tg wait $process | complete
success $output 'the process should finish after an OOM kill'
let outcome = $output.stdout | from json
let error = tg get --depth inf --no-tokens $outcome.error
assert ($error | str contains 'the sandbox was killed by the out-of-memory killer') $error
assert ($error | str contains $'"memory_limit":"($memory)"') $error
