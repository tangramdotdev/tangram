use ../lib/test.nu *

# A process can check in the store path of a symlink target that a destructive check-in put in the store without an authorization search.

let server = server spawn --config {
	authorization: { final: false, initial: false }
	vfs: false
}

let directory = mktemp -d
tg checkin --destructive --no-ignore $directory

let path = artifact {
	tangram.ts: '
		export default async function () {
			const directory = await tg.directory({});
			return tg.build`tg checkin "\${SYMLINK%/*}/${directory.id}"`.env({
				SYMLINK: tg.symlink({ artifact: directory }),
			});
		}
	'
}

let output = tg build $path | complete
success $output "the process should check in the store path of its symlink target"
