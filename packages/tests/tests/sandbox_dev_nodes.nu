use ../util/test.nu *

# A container sandbox provides the standard devices and the host's optional FUSE and TUN devices, even when its dev mount replaces the current /dev.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let uid = ^id -u | str trim
let gid = ^id -g | str trim
mut program = '[ -c /dev/null ] && [ -c /dev/zero ] && [ -c /dev/full ] && [ -c /dev/random ] && [ -c /dev/urandom ] && [ -c /dev/tty ]'
for device in [/dev/fuse /dev/net/tun] {
	if not ($device | path exists) {
		$program += $' && [ ! -e ($device) ]'
		continue
	}
	$program += $' && [ -c ($device) ]'

	# Devices that can be opened on the host must also be usable inside the sandbox.
	let output = ^sh -c 'exec 3<>"$1"' -- $device | complete
	if $output.exit_code == 0 {
		$program += $' && exec 3<>($device)'
	}
}
let output = tg sandbox run --backend builtin --bind / / --dev /dev --gid $gid --uid $uid --unshare-all -- /bin/sh -c $program | complete
success $output
