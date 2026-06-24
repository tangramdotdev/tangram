use ../../test.nu *

# A build whose return value is a self-referential object fails because of the cyclic value.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			let x = {};
			x.a = x;
			return x;
		}
	'
}

let output = tg build $path | complete
failure $output
