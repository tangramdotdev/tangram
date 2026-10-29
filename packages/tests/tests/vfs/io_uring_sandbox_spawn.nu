use ../../lib/test.nu *

# Many concurrent sandboxed processes spawn through per-sandbox FUSE mounts that use the automatic io_uring transport.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if not (fuse_io_uring_available) {
	skip_test 'this test requires FUSE io_uring support'
}

let local = server spawn --config {
	# Allow room for the process records and permissions from 256 concurrent sandboxes.
	index: { map_size: 67_108_864 }
	# Admit 256 concurrent sandboxes with the default resource requests regardless of the host's capacity.
	runner: {
		overcommit: { cpu: 256, memory: 1024 },
	}
	vfs: {
		io: 'auto'
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
