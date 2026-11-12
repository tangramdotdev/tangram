use ../../util/test.nu *

# An idle sandbox accrues reservation time for its CPU minimum and no consumption time for its capacity above the minimum.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux CPU limits'
}

let directory = project_quota_directory
let local = server spawn --directory $directory --config {
	runner: {
		memory_sampling_interval: 0.05,
		sandbox_pool_size: 0,
	},
}

let sandbox = tg sandbox create --no-tokens --no-time-to-live --cpu-min 0.25 --cpu-max 1 | referent node
sleep 200ms
tg sandbox destroy --wait $sandbox
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.cpu { max: 1, min: 0.25 }
assert ($data.usage.cpu.reservation >= 50) 'an idle minimum must accrue reservation time'
assert ($data.usage.cpu.consumption < 200) 'idle capacity above the minimum must not accrue consumption time'
