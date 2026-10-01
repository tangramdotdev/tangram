use ../lib/test.nu *

# Transitive file dependencies must be readable by store path without opening intermediate files or searching the authorization graph.

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

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
let configs = [{ vfs: false }] | append ($transports | each { |io| { vfs: { io: $io, kind: fuse, passthrough: disabled } } })
for config in $configs {
	let server = server spawn --config ($config | merge { authorization: { final: false, initial: false } })
	let command = tg build $path | str trim
	let output = tg run --sandbox $command | complete
	success $output $'the executable must read its transitive file dependency with ($config | to nuon)'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $server
}
