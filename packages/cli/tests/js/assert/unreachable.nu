use ../../../test.nu *

# tg.unreachable throws the default message.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.unreachable();
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"reached unreachable code"'
