use ../lib/test.nu *

# An executable reads its file dependency by store path without authorization search.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --name local --config {
		authorization: { final: false, initial: false }
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}

	let module = artifact {
		tangram.ts: '
			export default async function () {
				const dependency = await tg.file("dependency\n");
				const executable = await tg.file({
					contents: `#!/bin/sh\nIFS= read -r value < "\${0%/*}/${dependency.id}" && printf "%s\\n" "$value"\n`,
					dependencies: { [dependency.id]: dependency },
					executable: true,
				});
				return tg.command({ executable, host: tg.host.current });
			}
		'
	}

	let command = tg build $module | str trim
	let output = tg run --sandbox $command | complete
	success $output 'the executable must read its file dependency through the VFS with authorization search disabled'
	assert equal ($output.stdout | str trim) 'dependency'
	server stop $local
}
