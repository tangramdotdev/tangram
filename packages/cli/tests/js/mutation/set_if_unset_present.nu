use ../../../test.nu *

# Applying a set-if-unset mutation leaves an existing value unchanged.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let map = { k: "old" };
			await (await tg.Mutation.setIfUnset("new")).apply(map, "k");
			return map;
		}
	'
}

let output = tg build $path
snapshot $output '{"k":"old"}'
