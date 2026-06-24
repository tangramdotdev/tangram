use ../../../test.nu *

# tg.blob creates a blob from a byte array.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await (await tg.blob(tg.encoding.utf8.encode("bytes!"))).text; }'
}

let output = tg build $path
snapshot $output '"bytes!"'
