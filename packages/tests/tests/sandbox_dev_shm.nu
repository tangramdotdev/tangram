use ../util/test.nu *

# A container sandbox has a tmpfs on /dev/shm. LMDB guards its reader table with POSIX semaphores, which are files there, so a server started in a sandbox fails to open its index without it.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let directory = mktemp --directory
let uid = ^id -u | str trim
let gid = ^id -g | str trim
let script = $'tangram --directory "($directory)" --no-remotes server start; tangram --directory "($directory)" server stop'
let output = tg sandbox run --backend builtin --bind / / --dev /dev --gid $gid --uid $uid --unshare-all -- /bin/sh -ec $script | complete

# The server reports why it failed to start in its log rather than to the client.
let log = try { open --raw ($directory | path join 'log') } catch { '' }
success $output $"the server failed to start in the sandbox: ($log)"
