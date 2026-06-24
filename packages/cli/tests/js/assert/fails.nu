use ../../../test.nu *

# tg.assert throws the default message when the condition is falsy.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			try {
				tg.assert(false);
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
