use ../../../test.nu *

# A directory's entries accessor returns its child names mapped to artifacts.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": "alpha", "b": "beta" });
			return Object.keys(await directory.entries).sort();
		}
	'
}

let output = tg build $path
snapshot $output '["a","b"]'
