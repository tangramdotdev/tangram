use ../test.nu *

# A sandbox can be created with hostname, mount, and network options, listed with those options reflected, and destroyed so that it is eventually finalized and no longer found.

let server = spawn

let create = if $nu.os-info.name == 'linux' {
	tg sandbox create --hostname sandbox-test --mount /tmp:/sandbox,ro --no-network
} else {
	tg sandbox create --mount /tmp:/sandbox,ro --no-network
}
let create = $create | str trim
assert ($create | str starts-with "sbx_")

let list = tg sandbox list | from json
let sandbox = ($list | where id == $create | first)
if $nu.os-info.name == 'linux' {
	assert ($sandbox.hostname == "sandbox-test")
}
assert (($sandbox.mounts | first) == "/tmp:/sandbox,ro")
assert (($sandbox.network? | is-empty))

tg sandbox destroy $create

let list = tg sandbox list | from json
assert (($list | where id == $create | is-empty))

wait_until { (tg sandbox get $create | complete | get exit_code) != 0 } "the sandbox should be finalized"
let output = tg sandbox get $create | complete
failure $output "the sandbox should be finalized"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the sandbox
	   sandbox = <sandbox>

'
