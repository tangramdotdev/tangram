use ../../test.nu *
use ../lib/lsp.nu

# A tagged file module uses the explicit @module suffix so literal tags ending in module-like suffixes remain unambiguous.

let server = spawn

let dep_path = artifact {
	dep.tg.ts: '
		export const foo = () => "foo";
	'
}
tg tag dep.tg.ts ($dep_path | path join dep.tg.ts)
let tag_path = $server.directory | path join store dep.tg.ts
assert (not ($tag_path | path exists --no-symlink)) "expected putting the tag not to create its store entry"

let path = artifact {
	tangram.ts: '
		import { foo } from "dep.tg.ts";
		export default () => foo();
	'
}

let module_path = $path | path join tangram.ts
let module_uri = lsp uri $module_path
let source = open $module_path

mut client = lsp start
$client = lsp send_all $client [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $module_uri $source)
	(lsp document_link 10 $module_uri)
]

let output = lsp wait_result $client 10
$client = $output.session
let links = $output.result
assert (($links | length) == 1) "expected one document link"
let uri = $links.0.target
snapshot --normalize-ids --redact $server.directory $uri 'file://<redacted>/store/dep.tg.ts@module.tg.ts'

let module_path = lsp path_for_uri $uri
assert ($module_path | path exists) "expected the tagged file module to be materialized"
lsp stop $client

tg tag delete dep.tg.ts | ignore
assert (not ($module_path | path exists)) "expected deleting the tag to remove its module alias"
