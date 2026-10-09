use ../lib/test.nu *

# A container's cgroup limits its process population.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let cgroup_parent = container_cgroup_parent [pids]

let cgroup = $'tangram-test-(random uuid)'
let script = r#'
	set -eu
	cgroup="$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)"
	test "$(cat "/sys/fs/cgroup${cgroup}/pids.max")" = 32
'#
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup $cgroup --cgroup-pids 32 -- /bin/sh -c $script | complete
success $output
assert not ($cgroup_parent | path join $cgroup | path exists) 'the sandbox cgroup should be removed'
