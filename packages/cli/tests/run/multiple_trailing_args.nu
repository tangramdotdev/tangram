use ../../test.nu *

# Test that multiple trailing arguments are all passed to the process.

let server = spawn

let path = artifact {
	tangram.ts: 'export default (...args: string[]) => args.join(", ");'
}

let sandbox_output = tg run $path --sandbox -- a b c
snapshot $sandbox_output '"a, b, c"'

let output = tg run $path -- a b c
assert equal $output $sandbox_output
