use ../../../test.nu *

# A mutation's objects method returns the artifacts contained in its value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return (await tg.Mutation.set(await tg.file("hi"))).objects(); }'
}

let output = tg build $path
snapshot --normalize-ids $output '[fil_010000000000000000000000000000000000000000000000000000]'
