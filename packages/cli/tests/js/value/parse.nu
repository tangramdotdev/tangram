use ../../../test.nu *

# tg.Value.parse reads a value from its TGON text.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Value.parse("{\"a\":1,\"b\":[true,\"x\"]}");'
}

let output = tg build $path
snapshot $output '{"a":1,"b":[true,"x"]}'
