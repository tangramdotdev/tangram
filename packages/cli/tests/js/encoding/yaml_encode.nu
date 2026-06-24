use ../../../test.nu *

# tg.encoding.yaml.encode serializes a value to a YAML string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.yaml.encode({ name: "tangram", items: [1, 2] }); }'
}

let output = tg build $path
snapshot $output '"name: tangram\nitems:\n- 1\n- 2\n"'
