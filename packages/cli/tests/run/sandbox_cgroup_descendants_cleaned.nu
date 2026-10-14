use ../lib/test.nu *

# Container teardown kills a surviving descendant and removes its cgroup before returning.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let current_cgroup = ^awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup | str trim
let cgroup = $'tangram-test-(random uuid)'
let cgroup_path = $'/sys/fs/cgroup($current_cgroup)/($cgroup)'
let script = '/bin/sleep 60 &'
{ cgroup: { name: $cgroup }, process: { arguments: [/bin/sh -c $script] } } | insert kind builtin | wrap backend | to json | save setup.json
let output = ^timeout 10s tangram sandbox run --config setup.json | complete
success $output 'container teardown should not wait for a surviving descendant'
assert not ($cgroup_path | path exists) 'container teardown should remove the cgroup'
