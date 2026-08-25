use ../../../test.nu *

# Applying a prepend mutation inserts its values before an existing array.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await (await tg.Mutation.prepend(["a"])).apply(["b", "c"]);
		}
	'
}

let output = tg build $path
snapshot $output '["a","b","c"]'
