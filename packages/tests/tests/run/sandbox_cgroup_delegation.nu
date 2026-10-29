use ../../lib/test.nu *

# A sandbox can run a resource-limited nested container while retaining its outer limits.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --busybox
container_cgroup_parent [cpu memory] | ignore
let sandbox = tg sandbox create --cpu 1 --memory 268435456 --no-tokens | referent node

let script = r#'
	set -eu
	# The sandbox limits are outside the guest cgroup namespace.
	test "$(cat /sys/fs/cgroup/memory.max)" = max
	test "$(cat /proc/self/cgroup)" = 0::/init
	/opt/tangram/bin/tangram sandbox run --cgroup nested --cgroup-cpu 1 --cgroup-memory 134217728 -- /bin/sh -c 'echo nested'
'#
let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string, sandbox: string) => tg.run`${script}`
			.env(tg.build(busybox))
			.sandbox(sandbox);
	',
}
let output = tg run $command --arg-string $script --arg-string $sandbox | complete
success $output 'a resource-limited nested container should run'
assert equal ($output.stdout | str trim) 'nested'

# A process that exceeds the outer memory limit after the nested container ran is killed for exceeding the sandbox memory.
let script = 'value=x; while :; do value="$value$value"; done'
let process = tg spawn $"--sandbox=($sandbox)" --verbose --executable /bin/sh -- -c $script | from json | get process
let output = timeout 60s tg wait $process | complete
success $output 'the process should finish after an OOM kill'
let outcome = $output.stdout | from json
let error = tg get --depth inf --no-tokens $outcome.error
assert ($error | str contains 'the sandbox exceeded its memory max') $error
