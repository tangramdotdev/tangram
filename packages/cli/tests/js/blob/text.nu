use ../../../test.nu *

# A blob's text accessor returns its contents decoded as a string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await (await tg.blob("hello")).text; }'
}

let output = tg build $path
snapshot $output '"hello"'
