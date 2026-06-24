use ../../../test.nu *

# A file's read method returns its full contents as a byte array.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await (await tg.file("hello")).read(); }'
}

let output = tg build $path
snapshot $output 'tg.bytes("aGVsbG8=")'
