use ../../util/test.nu *

# A VFS server works where the FUSE control filesystem is unavailable, such as in a sandbox, whose user namespace cannot mount it and whose sysfs may be read-only.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if (which unshare | is-empty) {
	skip_test 'this test requires unshare'
}
if (^unshare --user --map-current-user --mount --keep-caps true | complete).exit_code != 0 {
	skip_test 'this test requires a user namespace'
}

let directory = mktemp --directory
let file_path = $directory | path join 'file'
'hello' | save $file_path

let script = '
	if mountpoint -q /sys/fs/fuse/connections; then
		UNAVAILABLE
	fi
	tangram --config "$1" --directory "$2" --no-remotes server start
	id=$(tangram --config "$1" --directory "$2" --no-remotes checkin --no-tokens --root "$3")
	cat "$2/store/${id%%\?*}"
	tangram --config "$1" --directory "$2" --no-remotes server stop
	! mountpoint -q "$2/store"
'
for io in [auto read_write] {
	let config_path = $directory | path join $'($io).json'
	{
		vfs: {
			# Require a usable transport even when the io_uring startup timeout is too short.
			fuse: { io_uring_startup_timeout: 0.000000001 }
			io: $io
			kind: 'fuse'
			passthrough: 'disabled'
			sqpoll: false
		}
	} | to json | save $config_path
	for unavailable in [
		'mount -t tmpfs tmpfs /sys/fs/fuse/connections'
		'mount -o remount,bind,ro,nosuid,nodev,noexec,relatime /sys/fs/fuse/connections'
	] {
		let server_path = mktemp --directory --tmpdir-path $directory
		let script = $script | str replace 'UNAVAILABLE' $unavailable
		let output = ^unshare --user --map-current-user --mount --keep-caps sh -ec $script sh $config_path $server_path $file_path | complete
		let log = try { open --raw ($server_path | path join 'log') } catch { '' }
		success $output $"the VFS server failed with ($io): ($log)"
		assert equal $output.stdout 'hello'
	}
}
