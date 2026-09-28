use ../lib/test.nu *

# A process can read a child build output through the VFS when its result lacks an exact token.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let server = server spawn --config {
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}

	let module = artifact {
		tangram.ts: '
			export default async function () {
				const file = await tg.build({
					args: ["-ec", `printf "output\n" > "$TANGRAM_OUTPUT"`],
					executable: "/bin/sh",
					host: tg.host.current,
				}).sandbox();
				return tg.run({
					args: ["-ec", `
						IFS= read -r value < "$1"
						test "$value" = output
						printf "ok\n"
					`, "sh", file],
					executable: "/bin/sh",
					host: tg.host.current,
				}).sandbox();
			}
		'
	}

	for attempt in 0..<2 {
		let output = tg run $module | complete
		success $output 'the VFS must fall back to authorization for the build output'
		assert equal ($output.stdout | str trim) 'ok'
	}
	server stop $server
}
