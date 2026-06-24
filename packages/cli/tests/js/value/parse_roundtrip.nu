use ../../../test.nu *

# tg.Value.parse inverts tg.Value.stringify.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			let value = { a: 1, b: [true, "x"] };
			return tg.Value.parse(tg.Value.stringify(value));
		}
	'
}

let output = tg build $path
snapshot $output '{"a":1,"b":[true,"x"]}'
