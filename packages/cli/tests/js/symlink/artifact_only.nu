use ../../../test.nu *

# A symlink created from an artifact has that artifact and no path.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let file = await tg.file("hi");
			let symlink = await tg.symlink(file);
			return [
				(await symlink.artifact) instanceof tg.File,
				(await symlink.path) === undefined,
			];
		};
	'
}

let output = tg build $path
snapshot $output '[true,true]'
