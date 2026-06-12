use ../../../test.nu *

# tg.Error.expect throws when the value is not an error.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			try {
				tg.Error.expect("not an error");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
