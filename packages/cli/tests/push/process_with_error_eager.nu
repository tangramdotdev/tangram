use ../../test.nu *
use ./process.nu test

# Create a module that throws an error.
let path = artifact {
	tangram.ts: r#'
		export default () => {
			throw new Error("whoops");
		};
	'#
}

test $path "--errors" "--eager"
