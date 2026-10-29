use ../../lib/test.nu *

let quota_root = $env.TANGRAM_TEST_PROJECT_QUOTA_PATH? | default ''
if $nu.os-info.name == 'linux' and ($quota_root | is-empty) {
	skip_test 'TANGRAM_TEST_PROJECT_QUOTA_PATH is required for sandbox storage accounting'
}

# The cpu and memory options are reflected when getting the sandbox.

# The cpu and memory options require container or vm isolation, which is unavailable under the seatbelt isolation used on macOS.
if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let directory = mktemp --directory --tmpdir-path $quota_root
let local = server spawn --directory $directory

let id = tg sandbox create --no-tokens --cpu 1 --memory 268435456 | referent node

let sandbox = tg sandbox get $id | from json | get data
assert equal $sandbox.cpu { max: 1 } "the cpu option should be reflected"
assert equal $sandbox.memory { max: 268435456 } "the memory option should be reflected"
assert equal $sandbox.status "started" "the sandbox should be started"

tg sandbox destroy $id
