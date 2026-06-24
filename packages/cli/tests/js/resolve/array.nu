use ../../../test.nu *

# tg.resolve resolves each promise contained in an array.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.resolve([Promise.resolve("a"), "b"]); }'
}

let output = tg build $path
snapshot $output '["a","b"]'
