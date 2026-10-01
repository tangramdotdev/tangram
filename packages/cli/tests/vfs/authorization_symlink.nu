use ../lib/test.nu *

# A process follows artifact symlinks, nested targets, and chains without authorization search.

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
				const file = await tg.file("target\n");
				const target = await tg.directory({ nested: { value: file } });
				const link = await tg.symlink({ artifact: target, path: "nested/value" });
				const directory = await tg.directory({
					chain: tg.symlink({ artifact: link }),
					direct: tg.symlink({ artifact: file }),
					nested: { link },
					relative: tg.symlink("nested/link"),
				});
				return tg.command({
					args: ["-ec", `
						for name in chain direct nested/link relative; do
							IFS= read -r value < "$1/$name"
							test "$value" = target
						done
						printf "ok\n"
					`, "sh", directory],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		'
	}

	let command = tg build $module | str trim
	let output = tg run --sandbox $command | complete
	success $output 'the process must read the artifacts through the VFS with authorization search disabled'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $local
}
