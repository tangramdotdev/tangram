use ../../../test.nu *

# The builder's entries method adds a map of entries.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory().entries({ "g": "via entries" });
			let file = await directory.get("g");
			return await (file as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"via entries"'
