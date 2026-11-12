use ../../util/test.nu *

# A sandbox keeps its files across a stop and start whether or not it was claimed from the pool.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux project quotas'
}
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
for size in [0 1] {
	let local = server spawn --root --config {
		advanced: { checkpoints: true },
		runner: { backend: { kind: builtin, storage_limits: true }, sandbox_pool_size: $size, storage: 536870912 },
	}
	let sandbox = if $size == 1 {
		let checkpoint = checkpoint watch runner.sandbox.pool.take
		let job = job spawn {
			let job_id = job id
			tg sandbox create --no-tokens --no-time-to-live ...$mounts | job send --tag $job_id 0
		}
		checkpoint pass $checkpoint | ignore
		checkpoint unwatch $checkpoint
		job recv --tag $job | referent node
	} else {
		tg sandbox create --no-tokens --no-time-to-live ...$mounts | referent node
	}
	let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'echo persisted > /tmp/file' | complete
	success $output 'the write should succeed'
	tg sandbox stop $sandbox
	tg sandbox start $sandbox
	let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'cat /tmp/file' | complete
	success $output 'the file should survive the restart'
	assert equal ($output.stdout | str trim) persisted
	tg sandbox destroy $sandbox
	tg sandbox wait $sandbox | ignore
	server stop $local
}
