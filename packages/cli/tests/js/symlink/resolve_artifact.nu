use ../../../test.nu *

# A symlink with an artifact and no path resolves to that artifact.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let file = await tg.file("hi");
			let symlink = await tg.symlink(file);
			return (await symlink.resolve()) instanceof tg.File;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
