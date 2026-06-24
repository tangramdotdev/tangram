use ../../../test.nu *

# tg.Value.isMap accepts a plain object and rejects an array or a byte string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return [tg.Value.isMap({ a: 1 }), tg.Value.isMap([1]), tg.Value.isMap(tg.encoding.utf8.encode("x"))]; }'
}

let output = tg build $path
snapshot $output '[true,false,false]'
