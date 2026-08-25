use ../../test.nu *
use ../lib/vfs.nu

# A file served by the VFS must support more than one concurrent open. The provider supplies a new
# backing descriptor for each open, and the kernel refuses the second one because the inode already
# has a different backing file, so the second open fails with EIO.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server_path = mktemp --directory
let server = server spawn --directory $server_path --config {
	vfs: {
		kind: 'fuse'
		passthrough: 'required'
	}
}
vfs assert_mounted $server_path

let id = tg checkin (artifact { file.txt: 'contents' }) | str trim
let path = vfs root $server_path $id | path join 'file.txt'

# Registering a backing descriptor requires CAP_SYS_ADMIN, which a build made by this harness does
# not have, so skip unless one open succeeds. Run the test with `--tangram-path` and a binary that
# holds the capability to exercise passthrough.
if (^sh -c $'exec 3< "($path)"' | complete).exit_code != 0 {
	skip_test 'this test requires FUSE passthrough support'
}

# Hold two descriptors on the file at the same time.
let output = ^sh -c $'exec 3< "($path)"; exec 4< "($path)"' | complete
assert ($output.exit_code == 0) $'expected two concurrent opens to succeed, got: ($output.stderr | str trim)'
