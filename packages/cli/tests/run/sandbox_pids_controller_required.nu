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
{ cgroup: { name: $unrestricted_cgroup }, process: { arguments: [/bin/true] } } | insert kind builtin | to json | save unrestricted.json
let output = ^tangram sandbox run --config unrestricted.json | complete
success $output

let restricted_cgroup = $'tangram-test-(random uuid)'
{ cgroup: { name: $restricted_cgroup, pids: 32 }, process: { arguments: [/bin/true] } } | insert kind builtin | to json | save restricted.json
let output = ^tangram sandbox run --config restricted.json | complete
failure $output
assert ($output.stderr | str contains 'no writable cgroup parent has the required controllers enabled')
