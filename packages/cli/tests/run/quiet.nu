use ../../test.nu *

# When --quiet is set, `tg run` should suppress the process info message on stderr.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "hello";'
}

let sandboxed_output = tg -q run $path --sandbox | complete
success $sandboxed_output
assert (not ($sandboxed_output.stderr | str contains "pcs_"))
snapshot $sandboxed_output.stdout '
	"hello"

'

let output = tg -q run $path | complete
success $output
assert (not ($output.stderr | str contains "pcs_"))
assert equal $output.stdout $sandboxed_output.stdout
