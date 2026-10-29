use ../../util/test.nu *

# A container's cgroup disables swap.

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
{ backend: { kind: builtin }, command: { args: [/bin/sh -c $script], env: { PATH: ($env.PATH | str join (char esep)) } }, options: { cgroup: { name: $cgroup, memory_oom_group: true } } } | to json | save setup.json
let output = ^tangram sandbox run --config setup.json | complete
success $output
assert not ($cgroup_parent | path join $cgroup | path exists) 'the sandbox cgroup should be removed'
