use ../../../test.nu *

# tg.Error.expect returns the value unchanged when it is an error.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await tg.Error.expect(tg.error("boom")).message; }'
}

let output = tg build $path
snapshot $output '"boom"'
