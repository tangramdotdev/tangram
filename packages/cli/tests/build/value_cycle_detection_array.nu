use ../../test.nu *

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
