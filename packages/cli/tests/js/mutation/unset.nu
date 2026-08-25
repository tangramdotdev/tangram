use ../../../test.nu *

# Applying an unset mutation removes the key from the map.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let map = { k: "old" };
			await tg.Mutation.unset().applyTo(map, "k");
			return map;
		}
	'
}

let output = tg build $path
snapshot $output '{}'
