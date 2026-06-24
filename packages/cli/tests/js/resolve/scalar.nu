use ../../../test.nu *

# tg.resolve returns an already-resolved scalar value unchanged.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.resolve("hello"); }'
}

let output = tg build $path
snapshot $output '"hello"'
