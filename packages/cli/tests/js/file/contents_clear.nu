use ../../../test.nu *

# Null clears inherited file contents.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.file("body");
			let file = await tg.file(base, { contents: null });
			return await file.text;
		}
	'
}

let output = tg build $path
snapshot $output '""'
