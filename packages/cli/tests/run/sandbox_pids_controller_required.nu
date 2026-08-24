use ../../test.nu *

# A container only requires the pids controller when it has a process limit.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let current_cgroup = ^awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup | str trim
let subtree_control_path = $'/sys/fs/cgroup($current_cgroup)/cgroup.subtree_control'
let controllers = open --raw $subtree_control_path | str trim | split row ' '
if 'pids' in $controllers {
	skip_test 'this test requires the pids cgroup controller to be disabled'
}

let unrestricted_cgroup = $'tangram-test-(random uuid)'
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup $unrestricted_cgroup -- /bin/true | complete
success $output

let restricted_cgroup = $'tangram-test-(random uuid)'
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup $restricted_cgroup --cgroup-pids 32 -- /bin/true | complete
failure $output
assert ($output.stderr | str contains 'the pids cgroup controller is not enabled for child cgroups')
