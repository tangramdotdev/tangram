use ../lib/test.nu *

# A configured container filesystem limit bounds the writable sandbox filesystem.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --config {
	runner: {
		isolation: {
			container: {
				max_filesystem_inodes: 4096,
				max_filesystem_size: 16777216,
			},
		},
	},
}

let output = tg run --sandbox --executable /bin/sh -- -c 'test "$(stat -f -c %T /tmp)" = tmpfs && echo ok' | complete
success $output "the sandbox should use a tmpfs for its writable filesystem"
assert equal ($output.stdout | str trim) "ok"

let output = tg run --sandbox --executable /bin/sh -- -c 'dd if=/dev/zero of=/tmp/full bs=1M count=32' | complete
failure $output "a write larger than the configured filesystem limit should fail"
