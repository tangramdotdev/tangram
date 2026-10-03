use ../lib/test.nu *

# Hardened container defaults preserve a plain sandboxed run and apply each resource boundary.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let current_cgroup = ^awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup | str trim
let subtree_control_path = $'/sys/fs/cgroup($current_cgroup)/cgroup.subtree_control'
let controllers = open --raw $subtree_control_path | str trim | split row ' '
if 'memory' not-in $controllers or 'pids' not-in $controllers {
	skip_test 'this test requires the memory and pids cgroup controllers to be enabled'
}

let local = server spawn --config {
	runner: {
		isolation: {
			container: {
				harden: true,
			},
		},
	},
}

let script = r#'
	set -eu
	test "$(ulimit -n)" = 4096
	cgroup="$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)"
	test "$(cat "/sys/fs/cgroup${cgroup}/memory.swap.max")" = 0
	test "$(cat "/sys/fs/cgroup${cgroup}/pids.max")" = 1024
	test "$(stat -f -c %T /tmp)" = tmpfs
	test "$(( $(stat -f -c %S /tmp) * $(stat -f -c %b /tmp) ))" = 1073741824
	test "$(stat -f -c %c /tmp)" = 262144
	echo ok
'#
let output = tg run --sandbox --executable /bin/sh -- -c $script | complete
success $output 'a plain sandboxed run should succeed with hardened defaults'
assert equal ($output.stdout | str trim) 'ok'
