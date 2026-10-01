use ../lib/test.nu *

# An artifact executable and a file backed by nested blob branches work without authorization search.

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
				const contents = await tg.blob(tg.blob("one\n", "two\n"), tg.blob("three\n", "four\n"));
				const file = await tg.file(contents);
				const executable = await tg.file(`#!/bin/sh
					set -e
					{
						for expected in one two three four; do
							IFS= read -r value
							test "$value" = "$expected"
						done
					} < "$1"
					printf "ok\n"
				`).executable(true);
				return tg.command({ args: [file], executable, host: tg.host.current });
			}
		'
	}

	let command = tg build $module | str trim
	let output = tg run --sandbox $command | complete
	success $output 'the artifact executable and blob branches must be readable without authorization search'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $local
}
