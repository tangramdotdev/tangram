use ../lib/test.nu *

# Hardened container defaults preserve a plain sandboxed run and apply each resource boundary.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root = $env.TANGRAM_TEST_PROJECT_QUOTA_PATH? | default ''
if ($root | is-empty) {
	skip_test 'TANGRAM_TEST_PROJECT_QUOTA_PATH is not set'
}
let directory = mktemp --directory --tmpdir-path $root

let cgroup_parent = container_cgroup_parent [cpu memory pids]

let local = server spawn --busybox --directory $directory --config {
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
	test "$(stat -f -c %T /tmp)" != tmpfs
	test "$(stat -f -c %T /dev/shm)" = tmpfs
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
