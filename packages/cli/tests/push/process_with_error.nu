use ../../test.nu *
use ./process.nu test

# Pushing a process that threw an error, with the errors flag, makes the failed process present and identical on the remote.

# Create a module that throws an error.
let path = artifact {
	tangram.ts: r#'
		export default function () {
			throw tg.error("whoops");
		}
	'#
}

test $path "--errors"
test $path "--errors" "--eager"
