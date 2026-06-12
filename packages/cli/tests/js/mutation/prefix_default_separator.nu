use ../../../test.nu *

# Applying a prefix mutation without a separator concatenates the template directly onto the value.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let map = { k: "world" };
			await (await tg.Mutation.prefix("hello")).apply(map, "k");
			return map.k.components;
		};
	'
}

let output = tg build $path
snapshot $output '["helloworld"]'
