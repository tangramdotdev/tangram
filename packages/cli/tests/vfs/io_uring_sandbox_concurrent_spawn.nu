use ../../test.nu *

# Every sandboxed process execs its executable through its own per-sandbox FUSE mount, so a build
# that runs many short-lived sandboxed processes creates and tears down that many FUSE connections.
# This exercises the readiness handshake and connection teardown ordering that keep one sandbox
# from observing ENOTCONN or ENODEV while another sandbox is being destroyed.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if not (fuse_io_uring_available) {
	skip_test 'this test requires FUSE io_uring support'
}

let server = spawn --config {
	vfs: {
		io: 'io_uring'
		kind: 'fuse'
		passthrough: 'disabled'
	}
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const script = tg.file("#!/bin/sh\necho \"$1\"", { executable: true });
			const outputs = await Promise.all(
				Array.from({ length: 256 }, (_, index) =>
					tg.run({ args: [String(index)], executable: script }).sandbox(),
				),
			);
			return outputs.length;
		}
	'
}

let output = tg run $path | complete
success $output 'every concurrent sandboxed process should spawn'
