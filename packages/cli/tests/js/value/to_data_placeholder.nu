use ../../../test.nu *

# tg.Value.toData wraps a placeholder under the placeholder kind.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Value.toData(tg.placeholder("foo"));'
}

let output = tg build $path
snapshot $output '{"kind":"placeholder","value":{"name":"foo"}}'
