use ../../../test.nu *

# tg.Object.expect throws when the value is not an object.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Object.expect({ a: 1 });
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
