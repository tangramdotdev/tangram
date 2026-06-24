use ../../../test.nu *

# tg.Value.toData passes a scalar value through unchanged.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.Value.toData(42); }'
}

let output = tg build $path
snapshot $output '42'
