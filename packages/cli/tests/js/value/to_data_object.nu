use ../../../test.nu *

# tg.Value.toData references an object by its id under the object kind.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.Value.toData(await tg.file("hi")); }'
}

let output = tg build $path | normalize_ids
snapshot $output '{"kind":"object","value":"fil_010000000000000000000000000000000000000000000000000000"}'
