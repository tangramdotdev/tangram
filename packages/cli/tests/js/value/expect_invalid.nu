use ../../../test.nu *

# tg.Value.expect throws when the value is not a valid value.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Value.expect(() => 1);
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
