use ../../../test.nu *

# Applying a set mutation returns its value.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await (await tg.Mutation.set("v")).apply(undefined);
		}
	'
}

let output = tg build $path
snapshot $output '"v"'
