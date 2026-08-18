use ../../test.nu *
use ./process.nu test

# Pushing a process that threw an error includes the error by default.

# Create a module that throws an error.
let path = artifact {
	tangram.ts: r#'
		export default function () {
			throw tg.error.sync("whoops");
		}
	'#
}

test $path
test $path "--eager"
