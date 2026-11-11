use ../../util/test.nu *

# A warmed sandbox begins charging only after its identity is assigned.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux cgroup accounting'
}
let local = server spawn --config {
	advanced: { checkpoints: true },
	runner: { memory_sampling_interval: 0.01, sandbox_pool_size: 1 },
}
let watch = tg checkpoint watch runner.sandbox.pool.take | from json | get watch
let job = job spawn {
	let job_id = job id
	tg sandbox create --no-tokens --no-ttl --memory-min 1048576 | job send --tag $job_id 0
}
success (tg checkpoint wait --timeout 30s runner.sandbox.pool.take $watch 0 | complete) 'the sandbox should come from the warm pool'

# Hold the claim before assignment while the physical sandbox remains alive.
sleep 10sec
let released_at = date now
tg checkpoint continue runner.sandbox.pool.take $watch 0
tg checkpoint unwatch runner.sandbox.pool.take $watch
let sandbox = job recv --tag $job | referent node
tg sandbox destroy --wait $sandbox
let elapsed = (date now) - $released_at
let usage = tg sandbox get $sandbox | from json | get data.usage
assert ($usage.cpu.consumption < 1000) 'startup CPU must be excluded from the assigned usage'

# The memory reservation charges the one mebibyte minimum for each millisecond of the assigned lifetime, which must fit within the time after the claim was released.
assert ($usage.memory.reservation > 0) 'the assigned sandbox should be charged its memory minimum'
assert ($usage.memory.reservation <= ($elapsed / 1ms)) 'pool wait time must be excluded from the assigned memory usage'
