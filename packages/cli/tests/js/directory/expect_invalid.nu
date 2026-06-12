use ../../../test.nu *

# tg.Directory.expect throws when the value is not a directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Directory.expect("x");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
