use ../../../test.nu *

# Applying an unset mutation removes the key from the map.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let map = { k: "old" };
			await tg.Mutation.unset().apply(map, "k");
			return map;
		};
	'
}

let output = tg build $path
snapshot $output '{}'
