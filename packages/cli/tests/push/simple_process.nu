use ../../test.nu *
use ./process.nu test

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default () => {
			return 5
		}
	'#
}

test $path "--eager"
test $path "--lazy"
test $path "--lazy" "--commands"
