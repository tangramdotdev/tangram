use ../../../test.nu *

# tg.Value.isArray distinguishes an array from a map.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => [tg.Value.isArray([1, 2]), tg.Value.isArray({ a: 1 })];'
}

let output = tg build $path
snapshot $output '[true,false]'
