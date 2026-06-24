use ../../../test.nu *

# tg.resolve resolves promises nested at every depth through arrays and objects.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.resolve(Promise.resolve([Promise.resolve("a"), { k: Promise.resolve("b") }])); }'
}

let output = tg build $path
snapshot $output '["a",{"k":"b"}]'
