use ../../../test.nu *

# Applying an append mutation creates a new array when the key is absent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let map = {};
			await (await tg.Mutation.append(["x"])).apply(map, "k");
			return map;
		};
	'
}

let output = tg build $path
snapshot $output '{"k":["x"]}'
