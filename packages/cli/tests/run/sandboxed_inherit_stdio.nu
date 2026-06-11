use ../../test.nu *

# A sandboxed process with inherited stdin, stdout, and stderr reads from the client's stdin and writes to the client's stdout and stderr.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.run`
				read line
				echo "stdout:$line"
				echo "stderr:$line" 1>&2
			`.sandbox().stdio("inherit");
		}
	',
}

let output = "hello\n" | tg run $path | complete
success $output
snapshot $output.stdout '
	stdout:hello

'
assert (($output.stderr | lines | last) == "stderr:hello")
