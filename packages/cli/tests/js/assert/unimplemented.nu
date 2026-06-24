use ../../../test.nu *

# tg.unimplemented throws the default message.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			try {
				tg.unimplemented();
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"reached unimplemented code"'
