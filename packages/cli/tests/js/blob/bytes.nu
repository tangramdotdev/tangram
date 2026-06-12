use ../../../test.nu *

# A blob's bytes accessor returns its contents as a byte array.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await (await tg.blob("hello")).bytes;'
}

let output = tg build $path
snapshot $output 'tg.bytes("aGVsbG8=")'
