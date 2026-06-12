use ../../../test.nu *

# tg.Blob.expect throws when the value is not a blob.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Blob.expect("x");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
