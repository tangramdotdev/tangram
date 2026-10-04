use ../lib/test.nu *

# A configured container filesystem limit bounds the writable sandbox filesystem.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --busybox --config {
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

let output = tg run $command --arg-string 'test "$(stat -f -c %T /tmp)" = tmpfs && echo ok' | complete
success $output "the sandbox should use a tmpfs for its writable filesystem"
assert equal ($output.stdout | str trim) "ok"

let output = tg run $command --arg-string 'dd if=/dev/zero of=/tmp/full bs=1M count=32' | complete
failure $output "a write larger than the configured filesystem limit should fail"

# Shared memory consumes the same byte and inode budgets as the other writable directories.
let output = tg run $command --arg-string 'dd if=/dev/zero of=/dev/shm/full bs=1M count=32' | complete
failure $output 'shared memory should respect the filesystem byte limit'

let output = tg run $command --arg-string 'dd if=/dev/zero of=/tmp/first bs=1M count=8 && dd if=/dev/zero of=/dev/shm/second bs=1M count=12' | complete
failure $output 'tmp and shared memory should share one filesystem byte budget'

let output = tg run $command --arg-string 'set -e; i=0; while test $i -lt 5000; do : > /dev/shm/f$i; i=$((i+1)); done' | complete
failure $output 'shared memory should respect the filesystem inode limit'

# Network setup must complete before the child sends its filesystem descriptor.
let output = tg run $command --arg-string 'test "$(stat -f -c %T /tmp)" = tmpfs && echo ok' --arg-string true | complete
success $output 'a networked sandbox should start with filesystem limits'
assert equal ($output.stdout | str trim) 'ok'
