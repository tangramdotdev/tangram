use ../../../test.nu *

# tg.Value.print renders files and symlinks with absent optional fields without crashing and omits those fields.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let filePrint = tg.Value.print(await tg.file("hello"));
			let pathSymlink = tg.Value.print(await tg.symlink("a/b"));
			let target = await tg.file("target");
			let artifactSymlink = tg.Value.print(await tg.symlink({ artifact: target }));
			return (
				!filePrint.includes(`"module":`) &&
				!pathSymlink.includes(`"artifact":`) &&
				!artifactSymlink.includes(`"path":`)
			);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
