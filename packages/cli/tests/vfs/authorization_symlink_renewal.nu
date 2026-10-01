use ../lib/test.nu *

# A fresh symlink refreshes an already loaded target and its descendants without authorization search.

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
		object: { permission_time_to_live: 5 }
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}

	let module = artifact {
		tangram.ts: '
			export default async function (path: string) {
				const graph = await tg.graph({ nodes: [
					{ kind: "directory", entries: { link: 2, self: 0, value: 1 } },
					{ kind: "file", contents: "target\n" },
					{ kind: "symlink", artifact: 1 },
				] });
				const directory = await tg.directory({ graph, index: 0, kind: "directory" });
				const target = await tg.directory({ nested: directory });
				const source = await tg.directory({ link: tg.symlink({ artifact: target, path }) });
				return tg.command({
					args: ["-ec", `
						for name in value link self/self/link; do
							IFS= read -r value < "$1/link/$name"
							test "$value" = target
						done
						printf "ok\n"
					`, "sh", source],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		'
	}

	let first = tg build $module --arg-string nested | str trim
	let second = tg build $module --arg-string 'nested/.' | str trim
	let sandbox = tg sandbox create --no-network | str trim
	let output = tg run $'--sandbox=($sandbox)' $first | complete
	success $output 'the first symlink must load the shared target'
	assert equal ($output.stdout | str trim) 'ok'
	sleep 6sec
	let output = tg run $'--sandbox=($sandbox)' $second | complete
	success $output 'the fresh symlink must refresh the cached target and graph descendants'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $local
}
