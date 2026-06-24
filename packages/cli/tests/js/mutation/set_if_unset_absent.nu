use ../../../test.nu *

# Applying a set-if-unset mutation sets the value when the key is absent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let map = {};
			await (await tg.Mutation.setIfUnset("new")).apply(map, "k");
			return map;
		}
	'
}

let output = tg build $path
snapshot $output '{"k":"new"}'
