use ../../test.nu *

# A container's cgroup limits its process population.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let current_cgroup = ^awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup | str trim
let subtree_control_path = $'/sys/fs/cgroup($current_cgroup)/cgroup.subtree_control'
let controllers = open --raw $subtree_control_path | str trim | split row ' '
if 'pids' not-in $controllers {
	skip_test 'this test requires the pids cgroup controller to be enabled'
}

let cgroup = $'tangram-test-(random uuid)'
let script = r#'
	set -eu
	cgroup="$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)"
	test "$(cat "/sys/fs/cgroup${cgroup}/pids.max")" = 32
'#
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup $cgroup --cgroup-pids 32 -- /bin/sh -c $script | complete
success $output
