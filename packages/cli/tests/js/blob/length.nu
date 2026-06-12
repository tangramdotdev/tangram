use ../../../test.nu *

# A blob's length accessor returns the byte length of its contents.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await (await tg.blob("hello")).length;'
}

let output = tg build $path
snapshot $output '5'
