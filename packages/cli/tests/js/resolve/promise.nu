use ../../../test.nu *

# tg.resolve awaits a promise to produce its resolved value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.resolve(Promise.resolve("hello")); }'
}

let output = tg build $path
snapshot $output '"hello"'
