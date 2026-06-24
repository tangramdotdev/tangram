use ../../../test.nu *

# A file's executable bit defaults to false.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await (await tg.file("hello")).executable; }'
}

let output = tg build $path
snapshot $output 'false'
