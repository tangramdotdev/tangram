use ../../../test.nu *

# tg.Directory.withId returns a directory that preserves the given id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": "alpha" });
			return tg.Directory.withId(directory.id).id === directory.id;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
