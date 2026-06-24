use ../../../test.nu *

# tg.Value.toData encodes a byte string as base64 under the bytes kind.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.Value.toData(tg.encoding.utf8.encode("hi")); }'
}

let output = tg build $path
snapshot $output '{"kind":"bytes","value":"aGk="}'
