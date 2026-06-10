use ../../test.nu *
use ./process.nu test

# Lazily pushing a simple process that returns a file, with the commands flag, makes the process along with its command and output present and identical on the remote.

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default () => {
			return tg.file("hello, world!")
		}
	'#
}

test $path "--lazy" "--commands"
