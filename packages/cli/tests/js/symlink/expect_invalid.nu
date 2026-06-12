use ../../../test.nu *

# tg.Symlink.expect throws when the value is not a symlink.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Symlink.expect("x");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
