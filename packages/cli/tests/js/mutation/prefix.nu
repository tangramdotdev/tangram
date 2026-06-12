use ../../../test.nu *

# Applying a prefix mutation prepends a template to the existing value, joined by the separator.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let map = { k: "world" };
			await (await tg.Mutation.prefix("hello", " ")).apply(map, "k");
			return map.k.components;
		};
	'
}

let output = tg build $path
snapshot $output '["hello world"]'
