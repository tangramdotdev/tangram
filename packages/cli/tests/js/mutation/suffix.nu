use ../../../test.nu *

# Applying a suffix mutation appends a template to the existing value, joined by the separator.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let value = await (await tg.Mutation.suffix("world", " ")).apply("hello");
			return value.components;
		}
	'
}

let output = tg build $path
snapshot $output '["hello world"]'
