use ../lib/test.nu *

# Repeatedly resolving a path through a symlink in an artifact costs about the same with the VFS as without it.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			const directory = await tg.directory({ link: tg.symlink("target"), target: { "file.txt": "file" } });
			const output = await tg.build`
				start=$EPOCHREALTIME
				i=0
				while [ $i -lt 100000 ]; do
					[ -e ${directory}/link/file.txt ] || exit 1
					i=$((i + 1))
				done
				end=$EPOCHREALTIME
				echo $(( (\${end%.*}\${end#*.} - \${start%.*}\${start#*.}) / 1000 )) > ${tg.output}
			`.env(tg.build(busybox));
			return (await tg.File.expect(output).text).trim();
		}
	'
}

def measure [config: record] {
	let server = server spawn --busybox --config $config
	let milliseconds = tg build $path | from json | into int
	server stop $server
	$milliseconds
}

let io = if (fuse_io_uring_available) { 'io_uring' } else { 'read_write' }
let vfs = measure { vfs: { io: $io, kind: 'fuse' } }
let disabled = measure { vfs: false }
print $'100000 symlink traversals: vfs=($vfs)ms disabled=($disabled)ms'
assert ($vfs <= $disabled * 2 + 50) $'the VFS took ($vfs)ms, the disabled VFS took ($disabled)ms'
