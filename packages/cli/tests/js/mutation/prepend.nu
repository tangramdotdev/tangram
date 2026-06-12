use ../../../test.nu *

# Applying a prepend mutation inserts its values before an existing array.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let map = { k: ["b", "c"] };
			await (await tg.Mutation.prepend(["a"])).apply(map, "k");
			return map;
		};
	'
}

let output = tg build $path
snapshot $output '{"k":["a","b","c"]}'
