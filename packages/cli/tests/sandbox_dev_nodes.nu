use ../test.nu *

# A container sandbox binds the host's device nodes into its dev mount. The dev mount must not shadow its own bind sources when the target is the current /dev.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let uid = ^id -u | str trim
let gid = ^id -g | str trim
let program = '[ -c /dev/null ] && [ -c /dev/zero ] && [ -c /dev/full ] && [ -c /dev/random ] && [ -c /dev/urandom ] && [ -c /dev/tty ]'
let output = tg sandbox container run --dev /dev --gid $gid --index 0 --uid $uid --unshare-all -- /bin/sh -c $program | complete
success $output
