use ../../test.nu *

# When no reference is given, `tg run` defaults to "." and uses the current directory.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "default reference";'
}

let sandbox_output = do { cd $path; tg run --sandbox } | complete
success $sandbox_output
assert ($sandbox_output.stdout | str contains "default reference")

let output = do { cd $path; tg run } | complete
success $output
assert equal $output.stdout $sandbox_output.stdout
