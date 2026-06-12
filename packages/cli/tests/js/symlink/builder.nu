use ../../../test.nu *

# The builder's artifact and path methods set the symlink's fields.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let file = await tg.file("hi");
			let symlink = await tg.symlink().artifact(file).path("p");
			return [await symlink.path, (await symlink.artifact) instanceof tg.File];
		};
	'
}

let output = tg build $path
snapshot $output '["p",true]'
