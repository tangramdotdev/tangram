use ../../../test.nu *

# tg.encoding.json.decode parses a JSON string into a value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.json.decode("{\"name\":\"tangram\",\"items\":[1,2,3]}"); }'
}

let output = tg build $path
snapshot $output '{"items":[1,2,3],"name":"tangram"}'
