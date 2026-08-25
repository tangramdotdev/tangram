use ../../../test.nu *

# Applying a set-if-unset mutation leaves an existing value unchanged.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await (await tg.Mutation.setIfUnset("new")).apply("old");
		}
	'
}

let output = tg build $path
snapshot $output '"old"'
