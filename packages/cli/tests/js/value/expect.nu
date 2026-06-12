use ../../../test.nu *

# tg.Value.expect returns the value unchanged when it is a valid value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Value.expect({ a: 1 });'
}

let output = tg build $path
snapshot $output '{"a":1}'
