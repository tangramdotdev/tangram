use ../../../test.nu *

# Applying a set-if-unset mutation returns the new value when the input is absent.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await (await tg.Mutation.setIfUnset("new")).apply(undefined);
		}
	'
}

let output = tg build $path
snapshot $output '"new"'
