use ../lib/test.nu *

# Hardened container defaults preserve a plain sandboxed run and apply each resource boundary.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let cgroup_parent = container_cgroup_parent [cpu memory pids]

let local = server spawn --busybox --config {
	runner: {
		isolation: {
			container: ({ harden: true } | merge (container_id_maps)),
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
	test "$(stat -f -c %i /tmp)" = "$(stat -f -c %i /dev/shm)"
	umask 077
	printf private > /tmp/private
	/opt/tangram/bin/tangram checkin /tmp/private > /dev/null
	echo ok
'#
let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string) => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox();
	',
}
let output = tg run $command --arg-string $script | complete
success $output 'a plain sandboxed run should succeed with hardened defaults'
assert equal ($output.stdout | str trim) 'ok'
