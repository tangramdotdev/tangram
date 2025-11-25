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

test $path "--lazy" "--commands"
