use ../../util/test.nu *

# Custom mounts over the parents of the mandatory mounts keep the sandbox working, and mounts that cannot coexist with them are rejected with a clear error.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux namespaces'
}
let backends = if (which bwrap | is-empty) { [builtin] } else { [builtin bubblewrap] }
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
for backend in $backends {
	for size in [0 1] {
		let local = server spawn --config {
			runner: { backend: { kind: $backend }, sandbox_pool_size: $size },
		}

		# Writable mounts work, and the sandbox's own programs and certificates stay available.
		for target in [/opt /opt/tangram] {
			let source = mktemp -d
			"marker\n" | save ($source | path join marker)
			let script = $'cat ($target)/marker && /opt/tangram/bin/tangram --version >/dev/null && test -d /opt/tangram/etc/ssl/certs'
			let output = tg run --no-tokens --sandbox ...$mounts --mount $'($source):($target),rw' --executable /bin/sh -- -c $script | complete
			success $output 'the mount and the sandbox programs should be available'
			assert equal ($output.stdout | str trim) marker
		}

		# Read-only mounts over the mandatory mounts are rejected.
		for target in [/opt /opt/tangram] {
			let source = mktemp -d
			let output = tg run --no-tokens --sandbox ...$mounts --mount $'($source):($target),ro' --executable /bin/true | complete
			failure $output 'the read-only mount should be rejected'
			assert ($output.stderr | str contains 'a read-only mount cannot contain /opt/tangram')
		}

		# Mounts that replace a mandatory mount are rejected.
		for target in [/opt/tangram/bin /opt/tangram/output /opt/tangram/socket /opt/tangram/store /opt/tangram/store/nested /socket] {
			let source = mktemp -d
			let output = tg run --no-tokens --sandbox ...$mounts --mount $'($source):($target),rw' --executable /bin/true | complete
			failure $output 'the mount should be rejected'
			assert ($output.stderr | str contains 'the mount target is reserved for the sandbox')
		}
		server stop $local
	}
}
