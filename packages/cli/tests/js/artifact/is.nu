use ../../../test.nu *

# tg.Artifact.is is true for directories, files, and symlinks and false for other objects.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": await tg.file("hi") });
			let file = await tg.file("hi");
			let symlink = await tg.symlink("a/b");
			let blob = await tg.blob("hi");
			return [
				tg.Artifact.is(directory),
				tg.Artifact.is(file),
				tg.Artifact.is(symlink),
				tg.Artifact.is(blob),
			];
		}
	'
}

let output = tg build $path
snapshot $output '[true,true,true,false]'
