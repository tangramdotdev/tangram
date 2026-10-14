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
{ cgroup: { name: $cgroup, pids: 32 }, process: { arguments: [/bin/sh -c $script] } } | insert kind builtin | to json | save setup.json
let output = ^tangram sandbox run --config setup.json | complete
success $output
assert not ($cgroup_parent | path join $cgroup | path exists) 'the sandbox cgroup should be removed'
