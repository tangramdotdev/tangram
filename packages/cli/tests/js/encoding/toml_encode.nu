use ../../../test.nu *

# tg.encoding.toml.encode serializes a table to a TOML string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.toml.encode({ name: "tangram", version: 1 }); }'
}

let output = tg build $path
snapshot $output '"name = \"tangram\"\nversion = 1\n"'
