use ../../../test.nu *

# tg.Placeholder.expect throws when the value is not a placeholder.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Placeholder.expect("not a placeholder");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
