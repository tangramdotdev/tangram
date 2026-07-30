use ../../test.nu *
use ./process.nu test

# Pushing a process that spawns a child process makes the process and its selected commands, children, and outputs present on the remote across the combinations of the commands and recursive flags, under both eager and lazy push.

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default async function () {
			let a = await tg.build(x)
			return 5
		}
		export async function x() {
			return tg.file("hello")
		}
	'#
}

test $path "--eager"
test $path "--lazy"
test $path "--process-commands" "--eager"
test $path "--process-commands" "--lazy"
test $path "--eager" "--process-children"
test $path "--lazy" "--process-children"
test $path "--eager" "--process-children" "--process-commands"
test $path "--lazy" "--process-children" "--process-commands"
