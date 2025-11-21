use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let artifact = await tg.directory({
				"hello.txt": "contents",
				"link": tg.symlink("./hello.txt"),
			});
			let archive = await tg.archive(artifact, "tar");
			let extracted = await tg.extract(archive);
			tg.assert(extracted.id === artifact.id);
		};
	'
}

run tg build $path
