use ../../util/test.nu *

# Warm sandboxes preserve the backend filesystem cap unless a request supplies storage bounds.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux project quotas'
}
let backends = if (which bwrap | is-empty) { [builtin] } else { [builtin bubblewrap] }
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
for backend in $backends {
	for size in [0 1] {
		let local = server spawn --root --config {
			advanced: { checkpoints: true },
			runner: { backend: { kind: $backend, max_filesystem_size: 33554432, storage_limits: true }, sandbox_pool_size: $size, storage: 536870912 },
		}
		for case in [
			{ args: [], result: limited },
			{ args: [--storage 134217728], result: expanded },
			{ args: [--storage-min 16777216], result: expanded },
		] {
			let sandbox = if $size == 1 {
				let checkpoint = checkpoint watch runner.sandbox.pool.take
				let job = job spawn {
					let job_id = job id
					tg sandbox create --no-tokens --no-ttl ...$case.args ...$mounts | job send --tag $job_id 0
				}
				checkpoint pass $checkpoint | ignore
				checkpoint unwatch $checkpoint
				job recv --tag $job | referent node
			} else {
				tg sandbox create --no-tokens --no-ttl ...$case.args ...$mounts | referent node
			}
			let script = r#'
				set -eu
				dd if=/dev/zero of=/tmp/data bs=1048576 count=16
				rm /tmp/data
				if dd if=/dev/zero of=/tmp/data bs=1048576 count=64; then
					echo expanded
				else
					echo limited
				fi
			'#
			let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c $script | complete
			success $output 'the sandbox should enforce its effective filesystem maximum'
			assert equal ($output.stdout | str trim) $case.result
			if $case.result == limited {
				assert ($output.stderr | str contains 'Disk quota exceeded')
			}
			tg sandbox destroy $sandbox
			tg sandbox wait $sandbox | ignore
		}
		server stop $local
	}
}
