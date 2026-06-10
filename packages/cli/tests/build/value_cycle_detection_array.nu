use ../../test.nu *

# A build whose return value is a self-referential array fails because of the cyclic value.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			let x = [];
			x[0] = x;
			return x;
		};
	'
}

let output = tg build $path | complete
failure $output
