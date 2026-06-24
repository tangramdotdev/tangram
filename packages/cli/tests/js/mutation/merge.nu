use ../../../test.nu *

# Applying a merge mutation combines its entries into an existing map, keeping existing keys.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let map = { k: { a: 1 } };
			await (await tg.Mutation.merge({ b: 2 })).apply(map, "k");
			return map;
		}
	'
}

let output = tg build $path
snapshot $output '{"k":{"a":1,"b":2}}'
