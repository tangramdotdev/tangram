use ../../../test.nu *

# A null path override clears the inherited symlink path via the fluent builder.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let dir = await tg.directory({ "f": "x" });
			let base = await tg.symlink({ artifact: dir, path: "f" });
			let cleared = await tg.symlink(base).path(null);
			return (await cleared.path) === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
