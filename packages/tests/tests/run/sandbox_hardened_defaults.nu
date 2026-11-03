use ../../util/test.nu *

# Hardened container defaults preserve a plain sandboxed run and apply each resource boundary.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --root --config {
	runner: {
		backend: ({ kind: builtin, harden: true } | merge (container_id_maps --root)),
	},
}

let script = r#'
	set -eu
	test "$(ulimit -n)" = 4096
	cgroup="$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)"
	test "$(cat "/sys/fs/cgroup${cgroup}/memory.swap.max")" = 0
	test "$(cat "/sys/fs/cgroup${cgroup}/pids.max")" = 1024
	test "$(stat -f -c %T /tmp)" != tmpfs
	test "$(stat -f -c %T /dev/shm)" = tmpfs
	umask 077
	printf private > /tmp/private
	/opt/tangram/bin/tangram checkin /tmp/private > /dev/null
	echo ok
'#
# Bind the host's programs into the sandbox read-only, because the server running as root cannot download busybox.
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
let output = tg run --sandbox ...$mounts --executable /bin/sh -- -c $'export PATH=/usr/bin:/bin; ($script)' | complete
success $output 'a plain sandboxed run should succeed with hardened defaults'
assert equal ($output.stdout | str trim) 'ok'
