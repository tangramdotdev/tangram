use ../../util/test.nu *

# A gVisor sandbox with the backend filesystem cap is claimed from the pool and keeps the cap.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux project quotas'
}
if (which runsc | is-empty) {
	skip_test 'runsc is not installed'
}
let source = which runsc | first | get path
let directory = project_quota_directory
let runsc = $directory | path join runsc
cp $source $runsc
chmod 755 $runsc
let sidecars = $source | path dirname | path join gvisor-bin
if ($sidecars | path exists) {
	cp -r $sidecars $directory
}
let local = server spawn --root --config {
	advanced: { checkpoints: true },
	runner: { backend: { kind: gvisor, max_filesystem_size: 33554432, path: $runsc, platform: systrap, storage_limits: true }, sandbox_pool_size: 1, storage: 536870912 },
}
let mounts = [/usr /lib /lib64 /bin] | where { path exists } | each {|path| [--mount $'($path | path expand):($path),ro'] } | flatten
let checkpoint = checkpoint watch runner.sandbox.pool.take
let job = job spawn {
	let job_id = job id
	tg sandbox create --no-tokens --no-ttl | job send --tag $job_id 0
}
checkpoint pass $checkpoint | ignore
checkpoint unwatch $checkpoint
let sandbox = job recv --tag $job | referent node
tg sandbox destroy $sandbox
tg sandbox wait $sandbox | ignore
