use ../lib/test.nu *

# A process checks in the store path of a file dependency it reads through the VFS without authorization search.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let local = server spawn --busybox --name local --config {
		verification: {
			permissions: {
				final: false
				initial: false
			}
		}
		vfs: { io: $io, kind: fuse, passthrough: disabled }
	}

	let path = artifact {
		tangram.ts: r#'
			import busybox from "busybox";
			export default async function () {
				const dependency = await tg.directory({ lib: { "libfoo.so": tg.file("library\n") } });
				const file = await tg.file({ contents: "wrapper\n", dependencies: { dependency } });
				return tg.build`
					cat "$FILE" > /dev/null
					cat /opt/tangram/store/${dependency.id}/lib/libfoo.so > /dev/null
					tg checkin --no-tokens /opt/tangram/store/${dependency.id}/lib/libfoo.so > ${tg.output}
				`.env(tg.build(busybox), { FILE: file });
			}
		'#,
	}

	let output = tg build $path | complete
	success $output $'the process should check in the store path of the dependency it read over ($io) with authorization search disabled'
	assert ((tg read ($output.stdout | str trim)) | str ends-with 'path=lib/libfoo.so') 'the checkin should resolve the dependency subpath'
	server stop $local
}
