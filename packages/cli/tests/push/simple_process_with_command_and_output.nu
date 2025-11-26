use ../../test.nu *
use ./process.nu test

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default () => {
			return tg.file("hello, world!")
		}
	'#
}

test $path "--lazy" "--commands"
