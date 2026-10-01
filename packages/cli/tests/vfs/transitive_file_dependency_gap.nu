use ../lib/test.nu *

# An executable reads a transitive file dependency by store path without an authorization search, with the VFS as without it.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const leaf = await tg.file("leaf\n");
			const middle = await tg.file({ contents: "middle\n", dependencies: { [leaf.id]: leaf } });
			const executable = await tg.file({
				contents: `#!/bin/sh
					set -e
					IFS= read -r value < "\${0%/*}/${leaf.id}"
					test "$value" = leaf
					printf "ok\n"
				`,
				dependencies: { [middle.id]: middle },
				executable: true,
			});
			return tg.command({ executable, host: tg.host.current });
		}
	'
}

let io = if (fuse_io_uring_available) { 'io_uring' } else { 'read_write' }
for config in [{ vfs: false } { vfs: { io: $io, kind: 'fuse' } }] {
	let server = server spawn --config ($config | merge { authorization: { final: false, initial: false } })
	let command = tg build $path | str trim
	let output = tg run --sandbox $command | complete
	success $output $'the executable must read its transitive file dependency with ($config | to nuon)'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $server
}
