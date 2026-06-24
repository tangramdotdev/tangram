use ../../../test.nu *

# tg.File.expect returns the value unchanged when it is a file.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.File.expect(await tg.file("hello")) instanceof tg.File; }'
}

let output = tg build $path
snapshot $output 'true'
