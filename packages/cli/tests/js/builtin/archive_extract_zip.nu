use ../../../test.nu *

# tg.archive and tg.extract round-trip a directory through the zip format.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a.txt": "alpha", "b.txt": "beta" });
			let archived = await tg.archive(directory, "zip");
			let extracted = (await tg.extract(archived)) as tg.Directory;
			return await ((await extracted.get("a.txt")) as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"alpha"'
