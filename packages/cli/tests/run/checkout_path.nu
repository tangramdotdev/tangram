use ../../test.nu *

# When --checkout=<path> is set, `tg run` should check out the output artifact to the specified path.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("checkout to path");'
}

let sandbox_dir = mktemp -d
let sandbox_checkout = ($sandbox_dir | path join "sandbox_output.txt")
let sandbox_output = tg run $"--checkout=($sandbox_checkout)" $path --sandbox | str trim
assert ($sandbox_output | str ends-with "sandbox_output.txt")
let sandbox_contents = open $sandbox_checkout
assert ($sandbox_contents == "checkout to path")

let unsandboxed_dir = mktemp -d
let unsandboxed_checkout = ($unsandboxed_dir | path join "output.txt")
let output = tg run $"--checkout=($unsandboxed_checkout)" $path | str trim
assert ($output | str ends-with "output.txt")
let contents = open $unsandboxed_checkout
assert ($contents == "checkout to path")
