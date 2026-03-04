use ../../test.nu *

# When --checkout-force is set with --checkout=<path>, `tg run` should overwrite an existing file at the path.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("new contents");'
}

let sandbox_dir = mktemp -d
let sandbox_checkout = ($sandbox_dir | path join "output.txt")
"old contents" | save $sandbox_checkout
tg run $"--checkout=($sandbox_checkout)" --checkout-force $path --sandbox
let sandbox_contents = open $sandbox_checkout
assert ($sandbox_contents == "new contents")

let unsandboxed_dir = mktemp -d
let unsandboxed_checkout = ($unsandboxed_dir | path join "output.txt")
"old contents" | save $unsandboxed_checkout
tg run $"--checkout=($unsandboxed_checkout)" --checkout-force $path
let contents = open $unsandboxed_checkout
assert ($contents == "new contents")
