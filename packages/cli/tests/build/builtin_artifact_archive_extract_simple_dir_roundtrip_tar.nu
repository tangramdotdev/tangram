use ../../test.nu *

# Archiving a directory to a tar and then extracting it yields an artifact identical to the original.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let artifact = await tg.directory({
				"hello.txt": "contents",
				"link": tg.symlink("./hello.txt"),
			});
			let archive = await tg.archive(artifact, "tar");
			let extracted = await tg.extract(archive);
			tg.assert(extracted.id === artifact.id);
		}
	'
}

tg build $path
