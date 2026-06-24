use ../../../test.nu *

# A symlink with a directory artifact and a path resolves to the entry at that path.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "sub": "deep" });
			let symlink = await tg.symlink({ artifact: directory, path: "sub" });
			let resolved = await symlink.resolve();
			return await (resolved as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"deep"'
