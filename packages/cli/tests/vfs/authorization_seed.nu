use ../lib/test.nu *

# The runner seeds existing executable, argument, and environment tokens before starting the process.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --name local --config {
		verification: {
			permissions: {
				final: false
				initial: false
			}
		}
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}
	let module = artifact {
		tangram.ts: '
			export default async function () {
				const executable = await tg.file(`#!/bin/sh
					set -e
					IFS= read -r value < "$1/nested/value"
					test "$value" = argument
					IFS= read -r value < "$INPUT"
					test "$value" = environment
					printf "ok\n"
				`).executable(true);
				const argument = await tg.directory({ nested: { value: tg.file("argument\n") } });
				const environment = await tg.file("environment\n");
				return tg.run({
					args: [argument],
					env: { INPUT: tg`${environment}` },
					executable,
					host: tg.host.current,
				}).sandbox();
			}
		'
	}
	let output = tg run $module | complete
	success $output 'seeded artifact tokens must authorize VFS access without a search'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $local
}
