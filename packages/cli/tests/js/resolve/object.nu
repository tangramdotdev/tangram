use ../../../test.nu *

# tg.resolve resolves each promise contained in an object's values.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.resolve({ k: Promise.resolve("v") }); }'
}

let output = tg build $path
snapshot $output '{"k":"v"}'
