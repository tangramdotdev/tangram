use ../../test.nu *

# When --pretty is set, `tg run` should pretty-print the output value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => ({ a: 1, b: 2 });'
}

let sandbox_output = tg run --pretty $path --sandbox
assert ($sandbox_output | str contains "\n")

let output = tg run --pretty $path
assert equal $output $sandbox_output
