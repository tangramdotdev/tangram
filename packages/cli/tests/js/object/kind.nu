use ../../../test.nu *

# tg.Object.kind returns the kind of an object instance.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": await tg.file("hi") });
			return tg.Object.kind(directory);
		}
	'
}

let output = tg build $path
snapshot $output '"directory"'
