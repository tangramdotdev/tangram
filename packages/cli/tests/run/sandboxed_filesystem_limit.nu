use ../lib/test.nu *

# A configured container filesystem limit bounds the writable sandbox filesystem.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root = $env.TANGRAM_TEST_PROJECT_QUOTA_PATH? | default ''
if ($root | is-empty) {
	skip_test 'TANGRAM_TEST_PROJECT_QUOTA_PATH is not set'
}
let directory = mktemp --directory --tmpdir-path $root

let local = server spawn --busybox --config {
	directory: $directory,
	runner: {
		isolation: {
			container: {
				max_filesystem_inodes: 4096,
				max_filesystem_size: 16777216,
			},
		},
	},
}

let command = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default (script: string, network = "false") => tg.run({
			executable: "/bin/sh",
			args: ["-c", script],
		}).env(tg.build(busybox)).sandbox().network(network === "true");
	',
}

let output = tg run $command --arg-string 'test "$(stat -f -c %T /tmp)" != tmpfs && echo ok' | complete
success $output "the sandbox should use a disk-backed writable filesystem"
assert equal ($output.stdout | str trim) "ok"

let output = tg run $command --arg-string 'dd if=/dev/zero of=/tmp/full bs=1M count=32' | complete
failure $output "a write larger than the configured filesystem limit should fail"

let output = tg run $command --arg-string 'set -e; i=0; while test $i -lt 5000; do : > /tmp/f$i; i=$((i+1)); done' | complete
failure $output 'the writable filesystem should respect the inode limit'

let output = tg run $command --arg-string 'test "$(stat -f -c %T /dev/shm)" = tmpfs && echo ok' | complete
success $output 'shared memory should remain tmpfs'

# Network setup must complete before the child sends its filesystem descriptor.
let output = tg run $command --arg-string 'test "$(stat -f -c %T /tmp)" != tmpfs && echo ok' --arg-string true | complete
success $output 'a networked sandbox should start with filesystem limits'
assert equal ($output.stdout | str trim) 'ok'
