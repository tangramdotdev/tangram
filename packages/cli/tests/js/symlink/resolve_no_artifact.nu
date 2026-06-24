use ../../../test.nu *

# Resolving a symlink that has a path but no artifact throws.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let symlink = await tg.symlink("a/b");
			try {
				await symlink.resolve();
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"cannot resolve a symlink with no artifact"'
