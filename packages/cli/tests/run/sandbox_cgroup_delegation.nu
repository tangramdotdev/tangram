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
	test "$(cat /sys/fs/cgroup/cpu.max)" = "100000 100000"
	test "$(cat /sys/fs/cgroup/memory.max)" = 268435456
	/opt/tangram/bin/tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup nested --cgroup-cpu 1 --cgroup-memory 134217728 -- /bin/sh -c 'echo nested'
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
success $output 'a resource-limited nested Tangram container should run'
assert equal ($output.stdout | str trim) 'nested'
let cgroup_paths = ls $cgroup_parent | where type == dir | get name | where { |path|
	$path | path join $sandbox | path exists
}
assert equal ($cgroup_paths | length) 1 'the nested cgroup should remain until sandbox teardown'
let cgroup_path = $cgroup_paths | first

tg sandbox destroy $sandbox
tg wait $sandbox
assert not ($cgroup_path | path exists) 'sandbox teardown should remove the delegated cgroup hierarchy'
