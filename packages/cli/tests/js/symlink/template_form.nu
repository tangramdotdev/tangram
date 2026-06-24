use ../../../test.nu *

# A symlink created from an artifact-and-path template splits into an artifact and a path.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "sub": "deep" });
			let symlink = await tg.symlink(await tg.template(directory, "/sub"));
			return [
				await symlink.path,
				(await symlink.artifact) instanceof tg.Directory,
			];
		}
	'
}

let output = tg build $path
snapshot $output '["sub",true]'
