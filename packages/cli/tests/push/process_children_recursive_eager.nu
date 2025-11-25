use ../../test.nu *
use ./process.nu test

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default async () => {
			let a = await tg.build(x)
			return 5
		}
		export let x = async () => {
			return tg.file("hello")
		}
	'#
}

test $path "--eager" "--recursive"
