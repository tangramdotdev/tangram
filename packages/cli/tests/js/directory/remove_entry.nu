use ../../../test.nu *

# A directory entry is removed by setting its value to null in a merge argument. Omitting the key would keep the entry, so a null value is distinct from absence.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.directory({ "a": "alpha", "b": "beta" });
			let result = await tg.directory(base, { "b": null });
			return Object.keys(await result.entries).sort();
		}
	'
}

let output = tg build $path
snapshot $output '["a"]'
