use ../test.nu *

let server = spawn

let create = (
	tg sandbox create
		--hostname sandbox-test
		--mount /tmp:/sandbox,ro
		--no-network
		--user nobody
)
let create = $create | str trim
assert ($create | str starts-with "sbx_")

let list = tg sandbox list | from json
let sandbox = ($list | where id == $create | first)
assert ($sandbox.hostname == "sandbox-test")
assert (($sandbox.mounts | first) == "/tmp:/sandbox,ro")
assert ($sandbox.network == false)
assert ($sandbox.user == "nobody")

tg sandbox delete $create

let list = tg sandbox list | from json
assert (($list | where id == $create | is-empty))

mut output = { exit_code: 0, stdout: '', stderr: '' }
for _ in 0..100 {
	$output = (tg sandbox get $create | complete)
	if $output.exit_code != 0 {
		break
	}
	sleep 0.05sec
}
failure $output "the sandbox should be finalized"
assert ($output.stderr | str contains 'failed to find the sandbox') "the error should mention the missing sandbox"
