use ../../../test.nu *

# tg.File.withId returns a file that preserves the given id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let file = await tg.file("hello");
			return tg.File.withId(file.id).id === file.id;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
