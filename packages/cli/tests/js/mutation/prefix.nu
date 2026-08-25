use ../../../test.nu *

# Applying a prefix mutation prepends a template to the existing value, joined by the separator.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let value = await (await tg.Mutation.prefix("hello", " ")).apply("world");
			return value.components;
		}
	'
}

let output = tg build $path
snapshot $output '["hello world"]'
