use ../../../test.nu *

# tg.assert throws the provided message when the condition is falsy.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			try {
				tg.assert(false, "custom message");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"custom message"'
