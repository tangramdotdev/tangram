use ../test.nu *

# A container sandbox has cgroup v2 mounted at /sys/fs/cgroup. A server creates a cgroup for every sandbox it spawns, so a server running in a sandbox cannot spawn one without it.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let uid = ^id -u | str trim
let gid = ^id -g | str trim

# Mask /sys with a tmpfs. A real sandbox overlays the rootfs onto /, where /sys is an empty directory, so without this the outer sandbox inherits the host's cgroup mount and the nested one succeeds for the wrong reason.
let output = tg sandbox container run --cgroup outer --dev /dev --gid $gid --index 0 --tmpfs /sys --uid $uid --unshare-all -- tangram sandbox container run --cgroup nested --gid $gid --index 0 --uid $uid --unshare-all -- /bin/sh -c 'exit 0' | complete
success $output 'a nested sandbox failed to create its cgroup'
