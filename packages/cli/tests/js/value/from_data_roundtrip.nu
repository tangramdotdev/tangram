use ../../../test.nu *

# tg.Value.fromData inverts tg.Value.toData.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			let value = { a: 1, b: [true, "x"] };
			return tg.Value.fromData(tg.Value.toData(value));
		}
	'
}

let output = tg build $path
snapshot $output '{"a":1,"b":[true,"x"]}'
