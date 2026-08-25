use ../../../test.nu *

# Applying a prefix mutation without a separator concatenates the template directly onto the value.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let value = await (await tg.Mutation.prefix("hello")).apply("world");
			return value.components;
		}
	'
}

let output = tg build $path
snapshot $output '["helloworld"]'
