use ../../test.nu *
use ./process.nu test

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default async () => {
			console.log("hello, stderr");
			let a = await tg.build(x)
			return 5
		}
		export let x = async () => {
			console.log("hello, stdout");
			return tg.file("hello")
		}
	'#
}

test $path "--lazy" "--recursive" "--commands"
