use ../../../test.nu *

# The builder's entry method adds a single entry at the given path.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory().entry("f", "via entry");
			let file = await directory.get("f");
			return await (file as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"via entry"'
