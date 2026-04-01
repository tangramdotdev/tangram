use ../test.nu *

let server = spawn

let create = (
	tg sandbox create
		--hostname sandbox-test
		--mount /tmp:/sandbox,ro
		--no-network
		--user nobody
	| complete
)
failure $create "The sandbox create command should fail until the server implementation exists."
assert (
	$create.stderr | str contains "sandbox creation is not implemented"
) "The sandbox create error should mention that creation is not implemented."

let list = (tg sandbox list | complete)
failure $list "The sandbox list command should fail until the server implementation exists."
assert (
	$list.stderr | str contains "sandbox listing is not implemented"
) "The sandbox list error should mention that listing is not implemented."

let delete = (tg sandbox delete sbx_0000000000000000000000000000 | complete)
failure $delete "The sandbox delete command should fail until the server implementation exists."
assert (
	$delete.stderr | str contains "sandbox deletion is not implemented"
) "The sandbox delete error should mention that deletion is not implemented."
