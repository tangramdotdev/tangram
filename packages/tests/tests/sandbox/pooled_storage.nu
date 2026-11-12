use ../../util/test.nu *

# A warm sandbox can receive a different storage maximum and still enforce it.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux project quotas'
}
let local = server spawn --root --config {
	advanced: { checkpoints: true },
	runner: { backend: { kind: builtin, storage_limits: true }, sandbox_pool_size: 1, storage: 536870912 },
	scheduler: { default_storage: 67108864 },
}
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
let checkpoint = checkpoint watch runner.sandbox.pool.take
let job = job spawn {
	let job_id = job id
	tg sandbox create --no-tokens --no-time-to-live --storage-min 16777216 --storage 134217728 ...$mounts | job send --tag $job_id 0
}
checkpoint pass $checkpoint | ignore
checkpoint unwatch $checkpoint
let sandbox = job recv --tag $job | referent node
let script = r#'
	set -eu
	dd if=/dev/zero of=/tmp/data bs=1048576 count=96
	rm /tmp/data
	if dd if=/dev/zero of=/tmp/data bs=1048576 count=160; then exit 1; fi
	rm /tmp/data
	echo limited
'#
let output = tg run --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c $script | complete
success $output 'the requested storage maximum should be enforced'
assert equal ($output.stdout | str trim) 'limited'
tg sandbox destroy $sandbox
tg sandbox wait $sandbox | ignore
let usage = tg sandbox get $sandbox | from json | get data.usage
assert ($usage.storage.reservation > 0) 'accounting should use the requested storage minimum'
