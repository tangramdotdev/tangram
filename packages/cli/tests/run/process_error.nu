use ../../test.nu *

# When a process produces an error, `tg run` should fail and print the error.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			throw new Error("something went wrong");
		};
	'
}

let sandbox_output = do { cd $path; tg run --sandbox } | complete
failure $sandbox_output
assert ($sandbox_output.stderr | str contains "the process failed")

let output = do { cd $path; tg run } | complete
failure $output
assert ($output.stderr | str contains "the process failed")
