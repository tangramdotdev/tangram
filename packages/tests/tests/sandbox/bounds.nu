use ../../lib/test.nu *

# An idle sandbox accrues reservation time for its CPU minimum and no consumption time for its capacity above the minimum.

let quota_root = $env.TANGRAM_TEST_PROJECT_QUOTA_PATH? | default ''
if $nu.os-info.name == 'linux' and ($quota_root | is-empty) {
	skip_test 'TANGRAM_TEST_PROJECT_QUOTA_PATH is required for sandbox storage accounting'
}

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux CPU limits'
}

let directory = mktemp --directory --tmpdir-path $quota_root
let local = server spawn --directory $directory --config {
	runner: {
		memory_sampling_interval: 0.05,
		sandbox_pool_size: 0,
	},
}

let sandbox = tg sandbox create --no-tokens --no-ttl --cpu-min 0.25 --cpu-max 1 | referent node
sleep 200ms
tg sandbox destroy $sandbox
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.cpu { max: 1, min: 0.25 }
assert ($data.usage.cpu.reservation >= 50) 'an idle minimum must accrue reservation time'
assert ($data.usage.cpu.consumption < 200) 'idle capacity above the minimum must not accrue consumption time'
