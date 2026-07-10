use ../../../test.nu *

# A null artifact override clears the inherited symlink artifact via the fluent builder.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let dir = await tg.directory({ "f": "x" });
			let base = await tg.symlink({ artifact: dir, path: "f" });
			let cleared = await tg.symlink(base).artifact(null);
			return (await cleared.artifact) === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
