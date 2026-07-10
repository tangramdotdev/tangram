use ../../../test.nu *

# The builder's artifact and path methods accept null to clear the symlink fields.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let file = await tg.file("hi");
			let symlink = await tg
				.symlink({ artifact: file, path: "p" })
				.artifact(null)
				.path(null);
			return [await symlink.artifact, await symlink.path];
		}
	'
}

let output = tg build $path
snapshot $output '[null,null]'
