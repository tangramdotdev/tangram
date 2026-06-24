use ../../../test.nu *

# A symlink created from a string has that path and no artifact.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let symlink = await tg.symlink("a/b");
			return [await symlink.path, (await symlink.artifact) === undefined];
		}
	'
}

let output = tg build $path
snapshot $output '["a/b",true]'
