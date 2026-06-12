use ../../../test.nu *

# tg.Command.expect throws when the value is not a command.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Command.expect("x");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
