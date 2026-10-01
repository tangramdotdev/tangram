use ../lib/test.nu *

# A process follows graph pointers, a directory cycle, and internal and external symlink targets without authorization search.

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
				const external = await tg.file("external\n");
				const graph = await tg.graph({ nodes: [
					{ kind: "directory", entries: { external: 3, link: 2, self: 0, value: 1 } },
					{ kind: "file", contents: "internal\n" },
					{ kind: "symlink", artifact: 1 },
					{ kind: "symlink", artifact: external },
				] });
				const directory = await tg.directory({ graph, index: 0, kind: "directory" });
				return tg.command({
					args: ["-ec", `
						for name in value link self/self/link; do
							IFS= read -r value < "$1/$name"
							test "$value" = internal
						done
						IFS= read -r value < "$1/self/external"
						test "$value" = external
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
