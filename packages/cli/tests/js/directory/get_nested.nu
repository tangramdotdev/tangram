use ../../../test.nu *

# A directory's get method resolves a nested path through intermediate directories.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "x/y/z": "deep" });
			let file = await directory.get("x/y/z");
			return await (file as tg.File).text;
		}
	'
}

let output = tg build $path
snapshot $output '"deep"'
