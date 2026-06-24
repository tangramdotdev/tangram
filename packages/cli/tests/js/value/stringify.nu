use ../../../test.nu *

# tg.Value.stringify serializes a value to its TGON text.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.Value.stringify({ a: 1, b: [true, "x"] }); }'
}

let output = tg build $path
snapshot $output '"{\"a\":1,\"b\":[true,\"x\"]}"'
