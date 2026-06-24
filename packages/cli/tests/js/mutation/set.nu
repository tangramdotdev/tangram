use ../../../test.nu *

# Applying a set mutation assigns the value to the key.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let map = {};
			await (await tg.Mutation.set("v")).apply(map, "k");
			return map;
		}
	'
}

let output = tg build $path
snapshot $output '{"k":"v"}'
