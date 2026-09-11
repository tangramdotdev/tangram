use ../../../test.nu *

# import.meta.module refers to the module's own source; empty referent options are omitted.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return import.meta.module; }'
}

let output = tg build $path
snapshot --normalize-ids $output 'tg.module({"kind":"ts","referent":{"node":fil_010000000000000000000000000000000000000000000000000000}})'
