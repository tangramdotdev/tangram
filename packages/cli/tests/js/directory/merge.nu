use ../../../test.nu *

# tg.directory merges multiple arguments into a single directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": "1" }, { "b": "2" });
			return Object.keys(await directory.entries).sort();
		}
	'
}

let output = tg build $path
snapshot $output '["a","b"]'
