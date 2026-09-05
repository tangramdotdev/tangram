use ../../test.nu *

# Checking in read-only files writes cache xattrs without changing the source permissions.

let server = server spawn

for mode in ['444', '555'] {
	let contents = $'read-only file ($mode)'
	let path = artifact $contents
	chmod $mode $path
	let permissions = ls -l $path | get mode | first

	let id = tg checkin $path
	let checkout = $server.checkout_directory | path join $id
	assert equal (tg read $id) $contents
	assert equal (ls -l $path | get mode | first) $permissions
	assert equal (ls -l $checkout | get mode | first) $permissions
	assert (not (xattr_read 'user.tangram.token' $checkout | is-empty)) 'missing cached file token'
}
