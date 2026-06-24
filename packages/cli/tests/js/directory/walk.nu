use ../../../test.nu *

# A directory's walk method yields every recursive entry with its joined path.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "x/y/z": "deep" });
			let paths = [];
			for await (let [name, _artifact] of directory.walk()) {
				paths.push(name);
			}
			return paths.sort();
		}
	'
}

let output = tg build $path
snapshot $output '["x","x/y","x/y/z"]'
