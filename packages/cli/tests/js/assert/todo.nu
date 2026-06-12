use ../../../test.nu *

# tg.todo throws a fixed message.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.todo();
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"reached todo"'
