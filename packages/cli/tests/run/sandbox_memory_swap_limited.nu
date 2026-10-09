use ../lib/test.nu *

# A container's cgroup disables swap when its swap limit is zero.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let cgroup_parent = container_cgroup_parent [memory]

let cgroup = $'tangram-test-(random uuid)'
let script = r#'
	set -eu
	cgroup="$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)"
	test "$(cat "/sys/fs/cgroup${cgroup}/memory.swap.max")" = 0
'#
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup $cgroup --cgroup-memory-swap 0 -- /bin/sh -c $script | complete
success $output
assert not ($cgroup_parent | path join $cgroup | path exists) 'the sandbox cgroup should be removed'
