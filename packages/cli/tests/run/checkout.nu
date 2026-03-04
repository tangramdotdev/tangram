use ../../test.nu *

# When --checkout is set without a path, `tg run` should check out the output artifact and print its path.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("checkout contents");'
}

let sandbox_output = tg run --checkout $path --sandbox | str trim
assert ($sandbox_output | str contains "/")
let sandbox_contents = open $sandbox_output
assert ($sandbox_contents == "checkout contents")

let output = tg run --checkout $path | str trim
assert ($output | str contains "/")
let contents = open $output
assert ($contents == "checkout contents")
