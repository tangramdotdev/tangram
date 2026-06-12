use ../../../test.nu *

# tg.Process.expect throws a failed assertion when the value is not a process.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			try {
				tg.Process.expect({});
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
