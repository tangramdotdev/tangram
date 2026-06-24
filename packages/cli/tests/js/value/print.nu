use ../../../test.nu *

# tg.Value.print renders a value compactly, preserving the key insertion order.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.Value.print({ b: 2, a: 1 }); }'
}

let output = tg build $path
snapshot $output '"{\"b\":2,\"a\":1}"'
