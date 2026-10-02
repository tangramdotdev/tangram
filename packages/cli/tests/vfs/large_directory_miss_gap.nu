use ../lib/test.nu *

# The VFS answers distinct missed lookups in a large checked-in directory quickly.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			const env = tg.build(busybox);
			const directory = tg.Directory.expect(await tg.build`
				mkdir ${tg.output}
				i=0
				while [ $i -lt 1025 ]; do
					echo $i > ${tg.output}/file$i.c
					i=$((i + 1))
				done
			`.env(env));
			tg.assert("children" in (await directory.object()), "expected a branch directory");
			const output = await tg.build`
				[ -f ${directory}/file0.c ] || exit 1
				start=$EPOCHREALTIME
				i=0
				while [ $i -lt 500 ]; do
					[ -e ${directory}/file$i.h ] && exit 1
					i=$((i + 1))
				done
				end=$EPOCHREALTIME
				echo $(( (\${end%.*}\${end#*.} - \${start%.*}\${start#*.}) / 1000 )) > ${tg.output}
			`.env(env);
			return Number((await tg.File.expect(output).text).trim());
		}
	'
}

let transports = if (fuse_io_uring_available) { [read_write io_uring] } else { [read_write] }
for io in $transports {
	let server = server spawn --busybox --config { vfs: { io: $io, kind: fuse } }
	let milliseconds = tg build $path | from json | into int
	server stop $server
	assert ($milliseconds < 2000) $'the VFS with ($io) took ($milliseconds)ms for 500 missed lookups'
}
