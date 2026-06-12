use ../../../test.nu *

# tg.File.expect throws when the value is not a file.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.File.expect("x");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
