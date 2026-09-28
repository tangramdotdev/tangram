use ../lib/test.nu *

# Renewing a parent token refreshes cached descendants and graph symlink targets without authorization search.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --name local --config {
		authorization: { final: false, initial: false }
		object: { permission_time_to_live: 5 }
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
						for entry in "$1/nested/"*; do :; done
						for name in value link self/self/link; do
							IFS= read -r value < "$1/nested/$name"
							test "$value" = internal
						done
						IFS= read -r value < "$1/nested/self/external"
						test "$value" = external
						printf "ok\n"
					`, "sh", tg.directory({ nested: directory })],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		'
	}

	let command = tg build $module | str trim
	let sandbox = tg sandbox create --no-network | str trim
	let output = tg run $'--sandbox=($sandbox)' $command | complete
	success $output 'the process must read the artifacts through the VFS with authorization search disabled'
	assert equal ($output.stdout | str trim) 'ok'
	sleep 6sec
	let output = tg run $'--sandbox=($sandbox)' $command | complete
	success $output 'renewed tokens must authorize cached graph descendants'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $local
}
