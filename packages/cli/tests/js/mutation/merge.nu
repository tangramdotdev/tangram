use ../../../test.nu *

# Applying a merge mutation combines its entries into an existing map, keeping existing keys.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await (await tg.Mutation.merge({ b: 2 })).apply({ a: 1 });
		}
	'
}

let output = tg build $path
snapshot $output '{"a":1,"b":2}'
