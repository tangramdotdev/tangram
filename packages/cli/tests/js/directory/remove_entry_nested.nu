use ../../../test.nu *

# A nested directory entry is removed by setting a path value to null, leaving sibling entries intact.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.directory({ "sub": { "b": "beta", "c": "gamma" } });
			let result = await tg.directory(base, { "sub/b": null });
			let sub = await result.get("sub");
			return Object.keys(await (sub as tg.Directory).entries).sort();
		}
	'
}

let output = tg build $path
snapshot $output '["c"]'
