use ../../../test.nu *

# tg.Graph.expect throws when the value is not a graph.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Graph.expect("x");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
