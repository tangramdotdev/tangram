use ../../../test.nu *

# tg.encoding.toml.decode parses a TOML string into a value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.toml.decode("name = \"tangram\"\nversion = 1\n"); }'
}

let output = tg build $path
snapshot $output '{"name":"tangram","version":1}'
