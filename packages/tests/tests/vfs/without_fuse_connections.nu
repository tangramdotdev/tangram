use ../../util/test.nu *

# A VFS server works where the FUSE control filesystem is unavailable, such as in a sandbox, whose user namespace cannot mount it.

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
let config_path = $directory | path join 'config.json'
{
	vfs: {
		kind: 'fuse'
		io: 'read_write'
		passthrough: 'disabled'
	}
} | to json | save $config_path
let file_path = $directory | path join 'file'
'hello' | save $file_path

# Hide the control filesystem, start the server, and read a checked in file through the VFS.
let server_path = $directory | path join 'server'
let script = '
	mount -t tmpfs tmpfs /sys/fs/fuse/connections
	tangram --config "$1" --directory "$2" --no-remotes server start
	id=$(tangram --config "$1" --directory "$2" --no-remotes checkin --no-tokens --root "$3")
	cat "$2/store/${id%%\?*}"
	tangram --config "$1" --directory "$2" --no-remotes server stop
'
let output = ^unshare --user --map-current-user --mount --keep-caps sh -ec $script sh $config_path $server_path $file_path | complete

# The server reports why it failed to start in its log rather than to the client.
let log = try { open --raw ($server_path | path join 'log') } catch { '' }
success $output $"the VFS server failed: ($log)"
assert equal $output.stdout 'hello'
