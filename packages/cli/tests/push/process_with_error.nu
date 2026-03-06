use ../../test.nu *
use ./process.nu test

# Create a module that throws an error.
let path = artifact {
	tangram.ts: r#'
		export default () => {
			throw tg.error("whoops");
		};
	'#
}

test $path "--errors"
test $path "--errors" "--eager"
