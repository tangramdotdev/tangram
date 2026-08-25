use ../../../test.nu *

# import.meta.module is a referent to the module's own source with empty options.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return import.meta.module; }'
}

let output = tg build $path
snapshot --normalize-ids $output 'tg.module({"kind":"ts","referent":{"node":fil_010000000000000000000000000000000000000000000000000000,"options":{}}})'
