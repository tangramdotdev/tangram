use ../../../test.nu *

# tg.encoding.yaml.decode parses a YAML string into a value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.encoding.yaml.decode("name: tangram\nitems:\n- 1\n- 2\n");'
}

let output = tg build $path
snapshot $output '{"items":[1,2],"name":"tangram"}'
