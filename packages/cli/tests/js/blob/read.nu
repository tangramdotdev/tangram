use ../../../test.nu *

# A blob's read method returns its full contents as a byte array.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await (await tg.blob("hello")).read(); }'
}

let output = tg build $path
snapshot $output 'tg.bytes("aGVsbG8=")'
