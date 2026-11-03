use ../../util/test.nu *

# A configured container filesystem limit bounds the writable sandbox filesystem.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --root --config {
	runner: {
		backend: {
			kind: builtin,
			max_filesystem_inodes: 4096,
			max_filesystem_size: 16777216,
		},
	},
}

# Bind the host's programs into the sandbox, because the server running as root cannot download busybox.
let mounts = [/usr /lib /lib64] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
let run = {|script: string, ...args: string|
	tg run --sandbox ...$mounts ...$args --executable /bin/sh -- -c $'export PATH=/usr/bin; ($script)' | complete
}

let output = do $run 'type=$(stat -f -c %T /tmp) && test "$type" != tmpfs && echo ok'
success $output "the sandbox should use a disk-backed writable filesystem"
assert equal ($output.stdout | str trim) "ok"

let output = do $run 'dd if=/dev/zero of=/tmp/full bs=1M count=32'
failure $output "a write larger than the configured filesystem limit should fail"
assert ($output.stderr | str contains 'Disk quota exceeded') 'the write should exceed the size quota'

let output = do $run 'set -e; i=0; while test $i -lt 5000; do : > /tmp/f$i; i=$((i+1)); done'
failure $output 'the writable filesystem should respect the inode limit'
assert ($output.stderr | str contains 'Quota exceeded') 'the file creation should exceed the inode quota'

let output = do $run 'type=$(stat -f -c %T /dev/shm) && test "$type" = tmpfs && echo ok'
success $output 'shared memory should remain tmpfs'

# Network setup must complete before the child sends its filesystem descriptor.
let output = do $run 'type=$(stat -f -c %T /tmp) && test "$type" != tmpfs && echo ok' '--network=true'
success $output 'a networked sandbox should start with filesystem limits'
assert equal ($output.stdout | str trim) 'ok'
