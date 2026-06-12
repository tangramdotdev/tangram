use ../../../test.nu *

# tg.Value.toData wraps a mutation under the mutation kind.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => tg.Value.toData(await tg.Mutation.set("v"));'
}

let output = tg build $path
snapshot $output '{"kind":"mutation","value":{"kind":"set","value":"v"}}'
