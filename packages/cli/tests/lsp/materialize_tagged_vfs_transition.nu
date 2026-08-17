use ../../test.nu *
use ../lib/lsp.nu

# A physical tag checkout left by a non-VFS run is removed when returning from the VFS to the physical store.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

def stop [server: record] {
	let pid = open ($server.directory | path join 'lock') | into int
	kill --signal 2 $pid
	wait_until { ps | where pid == $pid | is-empty } 'the server should stop'
}

let server_path = mktemp --directory
let server = spawn --directory $server_path

let dep_path = artifact {
	tangram.ts: 'export const foo = () => "foo";'
}
tg tag dep $dep_path

let path = artifact {
	tangram.ts: '
		import { foo } from "dep";
		export default () => foo();
	'
}
let module_path = $path | path join 'tangram.ts'
let module_uri = lsp uri $module_path
let source = open $module_path
let responses = lsp exchange [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $module_uri $source)
	(lsp definition 10 $module_uri 1 23)
]
lsp result $responses 10 | ignore

let tag_path = $server_path | path join 'store/dep'
assert ($tag_path | path exists) 'expected the physical tag alias'
stop $server

let server = spawn --directory $server_path --config { vfs: true }
tg tag delete dep | ignore
assert (($server_path | path join 'checkouts/dep') | path exists --no-symlink) 'expected the VFS not to maintain the backing tag checkout'
stop $server

spawn --directory $server_path | ignore
assert (not ($tag_path | path exists)) 'expected the deleted tag alias to stay absent without the VFS'
