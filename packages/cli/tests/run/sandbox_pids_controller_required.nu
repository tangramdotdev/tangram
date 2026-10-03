use ../lib/test.nu *

# A container only requires the pids controller when it has a process limit.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let current_cgroup = ^awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup | str trim
let current = $'/sys/fs/cgroup($current_cgroup)'
for parent in [$current ($current | path dirname)] {
	let path = $parent | path join cgroup.subtree_control
	if ($path | path exists) and ('pids' in (open --raw $path | str trim | split row ' ')) {
		skip_test 'this test requires the pids controller to be disabled in both candidate parents'
	}
}

let unrestricted_cgroup = $'tangram-test-(random uuid)'
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup $unrestricted_cgroup -- /bin/true | complete
success $output

let restricted_cgroup = $'tangram-test-(random uuid)'
let output = ^tangram sandbox container run --index 0 --unshare-all --uid 0 --gid 0 --chdir / --cgroup $restricted_cgroup --cgroup-pids 32 -- /bin/true | complete
failure $output
assert ($output.stderr | str contains 'no writable cgroup parent has the required controllers enabled')
