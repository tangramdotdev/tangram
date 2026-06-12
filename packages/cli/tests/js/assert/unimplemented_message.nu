use ../../../test.nu *

# tg.unimplemented throws the provided message.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.unimplemented("not yet");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"not yet"'
