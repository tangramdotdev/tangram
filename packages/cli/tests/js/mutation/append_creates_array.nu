use ../../../test.nu *

# Applying an append mutation to an absent value creates a new array.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await (await tg.Mutation.append(["x"])).apply(undefined);
		}
	'
}

let output = tg build $path
snapshot $output '["x"]'
