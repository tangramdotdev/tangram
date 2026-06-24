use ../../../test.nu *

# tg.Mutation.expect returns the value unchanged when it is a mutation.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let mutation = await tg.Mutation.set("v");
			return tg.Mutation.expect(mutation).inner.kind;
		}
	'
}

let output = tg build $path
snapshot $output '"set"'
