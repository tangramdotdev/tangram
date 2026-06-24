use ../../test.nu *
use ./process.nu test

# Pushing a simple process that returns a value makes the process present and identical on the remote, under both eager and lazy push and with the commands flag.

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default function () {
			return 5
		}
	'#
}

test $path "--eager"
test $path "--lazy"
test $path "--lazy" "--commands"
