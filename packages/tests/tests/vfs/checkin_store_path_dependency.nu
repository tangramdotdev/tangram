use ../../util/test.nu *

# A process checks in a dependency it has read through the VFS without an authorization search.

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

	let path = artifact {
		tangram.ts: r#'
			export default async function () {
				const dependency = await tg.file("dependency\n");
				const input = await tg.file({ contents: "input\n", dependencies: { dependency } });
				return tg.build`
					IFS= read -r value < "$INPUT" || exit 1
					IFS= read -r value < /opt/tangram/store/${dependency.id} || exit 1
					tg checkin --no-tokens /opt/tangram/store/${dependency.id} > ${tg.output}
				`.env({ INPUT: input });
			}
		'#,
	}

	let output = tg build $path | complete
	success $output $'the process should check in the store path of the dependency it read over ($io) with authorization search disabled'
	let reference = tg read ($output.stdout | str trim) | str trim
	assert equal (tg read $reference | str trim) 'dependency'
	server stop $local
}
