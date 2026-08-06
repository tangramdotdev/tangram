use ../../test.nu *

# Every sandboxed process execs its executable through its own per-sandbox FUSE mount. This ensures
# that the lightweight per-sandbox io_uring configuration stays within the process's locked memory
# limit under high concurrency. The runner capacity is set here so the number of concurrent
# sandboxes does not depend on the host's core count.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}
if not (fuse_io_uring_available) {
	skip_test 'this test requires FUSE io_uring support'
}

let server = spawn --config {
	runner: {
		cpus: 128,
		memory: (128e9 | into int),
	}
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
