use ../../../test.nu *

# import.meta.module is a referent to the module's own source with empty options.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return import.meta.module; }'
}

let output = tg build $path | normalize_ids
snapshot $output '{"kind":"ts","referent":{"item":fil_010000000000000000000000000000000000000000000000000000,"options":{}}}'
