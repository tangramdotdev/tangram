use ../../../test.nu *

# tg.archive accepts a compression format and round-trips through tg.extract.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a.txt": "alpha", "b.txt": "beta" });
			let archived = await tg.archive(directory, "tar", "gz");
			let extracted = (await tg.extract(archived)) as tg.Directory;
			return await ((await extracted.get("b.txt")) as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"beta"'
