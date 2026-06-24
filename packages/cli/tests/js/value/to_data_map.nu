use ../../../test.nu *

# tg.Value.toData wraps a plain object as a map.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.Value.toData({ a: 1 }); }'
}

let output = tg build $path
snapshot $output '{"kind":"map","value":{"a":1}}'
