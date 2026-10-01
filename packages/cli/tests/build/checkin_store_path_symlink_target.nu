use ../lib/test.nu *

# A process can check in the store path of an artifact it received as a symlink target without an authorization search.

let server = server spawn --config {
	verification: {
		permissions: {
			final: false
			initial: false
		}
	}
	vfs: false
}

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
