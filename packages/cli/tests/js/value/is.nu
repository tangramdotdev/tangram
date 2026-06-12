use ../../../test.nu *

# tg.Value.is accepts a valid value and rejects a function.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => [tg.Value.is({ a: [1, "x"] }), tg.Value.is(() => 1)];'
}

let output = tg build $path
snapshot $output '[true,false]'
