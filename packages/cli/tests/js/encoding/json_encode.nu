use ../../../test.nu *

# tg.encoding.json.encode serializes a value to a compact JSON string.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.json.encode({ name: "tangram", items: [1, 2, 3] }); }'
}

let output = tg build $path
snapshot $output '"{\"items\":[1,2,3],\"name\":\"tangram\"}"'
