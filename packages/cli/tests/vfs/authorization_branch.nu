use ../lib/test.nu *

# A process enumerates a branch directory and follows its artifact symlinks without authorization search.

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
				const children = [];
				for (let index = 0; index < 4; index++) {
					const entries = {};
					for (let offset = 0; offset < 32; offset++) {
						const name = String(index * 32 + offset).padStart(3, "0");
						entries[name] = tg.symlink({ artifact: tg.file(`${name}\n`) });
					}
					children.push({
						count: 32,
						directory: await tg.directory(entries),
						last: String(index * 32 + 31).padStart(3, "0"),
					});
				}
				const directory = await tg.directory({ children });
				return tg.command({
					args: ["-ec", `
						count=0
						for path in "$1"/*; do
							IFS= read -r value < "$path"
							test "$value" = "\${path##*/}"
							count=$((count + 1))
						done
						test "$count" = 128
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
