use ../lib/test.nu *

# A process reads nested and shared directory entries without authorization search, while unrelated artifacts remain inaccessible.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --name local --config {
		authorization: { final: false, initial: false }
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}

	let foreign = tg put 'tg.file("foreign\n")' | str trim

	let module = artifact {
		tangram.ts: '
			export default async function (foreign: string) {
				const shared = await tg.directory({ value: tg.file("shared\n") });
				const directory = await tg.directory({
					a: shared,
					b: shared,
					nested: { inner: { value: tg.file("nested\n") } },
				});
				return tg.command({
					args: ["-ec", `
						for name in a b; do
							IFS= read -r value < "$1/$name/value"
							test "$value" = shared
						done
						IFS= read -r value < "$1/nested/inner/value"
						test "$value" = nested
						if (IFS= read -r value < "$2") 2>/dev/null; then exit 1; fi
						printf "ok\n"
					`, "sh", directory, `/opt/tangram/store/${foreign}`],
					executable: "/bin/sh",
					host: tg.host.current,
				});
			}
		'
	}

	let command = tg build $module --arg-string $foreign | str trim
	let output = tg run --sandbox $command | complete
	success $output 'the process must read the artifacts through the VFS with authorization search disabled'
	assert equal ($output.stdout | str trim) 'ok'
	server stop $local
}
