use ../lib/test.nu *

# A sandbox can run a resource-limited nested container while retaining its outer limits.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --busybox
let cgroup_parent = container_cgroup_parent [cpu memory]
let sandbox = tg sandbox create --cpu 1 --memory 268435456 --no-tokens | referent node

let script = r#'
	set -eu
	# The sandbox limits are outside the guest cgroup namespace.
	test "$(cat /sys/fs/cgroup/memory.max)" = max
	test "$(cat /proc/self/cgroup)" = 0::/init
	/opt/tangram/bin/tangram sandbox run --cgroup nested --cgroup-cpu 1 --cgroup-memory 134217728 -- /bin/sh -c 'echo nested'
	# Leave a child cgroup for sandbox teardown to remove.
	mkdir /sys/fs/cgroup/nested
'# | str replace --all '/sys/fs/cgroup/nested' $'/sys/fs/cgroup/($sandbox)'
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
let cgroup_paths = ls $cgroup_parent | where type == dir | get name | where { |path|
	$path | path join guest $sandbox | path exists
}
assert equal ($cgroup_paths | length) 1 'the nested cgroup should remain until sandbox teardown'
let cgroup_path = $cgroup_paths | first
assert equal (open --raw ($cgroup_path | path join memory.max) | str trim) '268435456' 'the sandbox cgroup should retain the memory limit'
assert not ((open --raw ($cgroup_path | path join cpu.max) | str trim) starts-with 'max') 'the sandbox cgroup should retain the CPU limit'

tg sandbox destroy $sandbox
tg wait $sandbox
assert not ($cgroup_path | path exists) 'sandbox teardown should remove the delegated cgroup hierarchy'
