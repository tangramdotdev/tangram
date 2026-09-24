use ../lib/test.nu *

# Compare the elapsed time to read the same artifact symlinks with and without the VFS.

if not (fuse_io_uring_available) {
	skip_test 'this test requires FUSE io_uring support'
}

let module = artifact {
	tangram.ts: '
		export default async function () {
			const entries = {};
			for (let index = 0; index < 128; index++) {
				entries[index] = tg.symlink({ artifact: tg.file(`${index}\n`) });
			}
			const directory = await tg.directory(entries);
			return tg.command({
				args: ["-ec", "for path in \"$1\"/*; do IFS= read -r value < \"$path\"; test \"$value\" = \"${path##*/}\"; done", "sh", directory],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	'
}
# Use fresh servers for three samples of each configuration, including input checkout in the timing.
let measurements = [false true false true false true] | each { |vfs|
	let server = server spawn --config {
		vfs: (if $vfs { { kind: fuse, io: io_uring, passthrough: disabled } } else { false })
	}
	let command = tg build $module | str trim
	tg index
	let start = date now
	let output = tg run --sandbox $command | complete
	let elapsed = (date now) - $start
	success $output
	server stop $server
	{ vfs: $vfs, elapsed: $elapsed }
}
print $measurements
let without_vfs = $measurements | where vfs == false | get elapsed | math median
let with_vfs = $measurements | where vfs == true | get elapsed | math median
let slowdown = $with_vfs / $without_vfs
print $'VFS slowdown: ($slowdown)x'
assert ($slowdown <= 2) 'reading artifact symlinks is more than twice as slow with the VFS'
