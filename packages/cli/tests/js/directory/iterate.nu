use ../../../test.nu *

# A directory is an async iterator over its top-level entries.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": "alpha", "b": "beta" });
			let names = [];
			for await (let [name, _artifact] of directory) {
				names.push(name);
			}
			return names.sort();
		}
	'
}

let output = tg build $path
snapshot $output '["a","b"]'
