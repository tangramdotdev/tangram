use ../../../test.nu *

# Applying a suffix mutation appends a template to the existing value, joined by the separator.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let map = { k: "hello" };
			await (await tg.Mutation.suffix("world", " ")).apply(map, "k");
			return map.k.components;
		}
	'
}

let output = tg build $path
snapshot $output '["hello world"]'
