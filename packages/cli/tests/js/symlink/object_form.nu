use ../../../test.nu *

# A symlink created from an artifact-and-path object has both.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let directory = await tg.directory({ "sub": "deep" });
			let symlink = await tg.symlink({ artifact: directory, path: "sub" });
			return [
				await symlink.path,
				(await symlink.artifact) instanceof tg.Directory,
			];
		};
	'
}

let output = tg build $path
snapshot $output '["sub",true]'
