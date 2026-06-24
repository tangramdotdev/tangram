use ../../../test.nu *

# A file's read method honors the position and length options to return a slice.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await (await tg.file("hello")).read({ position: 1, length: 3 }); }'
}

let output = tg build $path
snapshot $output 'tg.bytes("ZWxs")'
