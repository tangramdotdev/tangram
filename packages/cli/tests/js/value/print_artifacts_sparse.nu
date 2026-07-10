use ../../../test.nu *

# tg.Value.print renders files and symlinks with absent optional fields without crashing and omits those fields.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			// A file with no module.
			let filePrint = tg.Value.print(await tg.file("hello"));

			// A symlink with a path and no artifact.
			let pathSymlink = tg.Value.print(await tg.symlink("a/b"));

			// A symlink with an artifact and no path.
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
