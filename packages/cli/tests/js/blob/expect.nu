use ../../../test.nu *

# tg.Blob.expect returns the value unchanged when it is a blob.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => tg.Blob.expect(await tg.blob("hello")) instanceof tg.Blob;'
}

let output = tg build $path
snapshot $output 'true'
