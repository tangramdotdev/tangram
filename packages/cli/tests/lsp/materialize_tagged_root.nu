use ../../test.nu *
use ../lib/lsp.nu

let server = spawn

let dep_path = artifact {
	tangram.ts: '
		export const foo = () => "foo";
	'
}
tg tag dep $dep_path

let path = artifact {
	tangram.ts: '
		import { foo } from "dep";
		export default () => foo();
	'
}

let module_path = $path | path join "tangram.ts"
let module_uri = lsp uri $module_path
let source = open $module_path

mut client = lsp start
$client = lsp send_all $client [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $module_uri $source)
	(lsp document_link 10 $module_uri)
	(lsp definition 11 $module_uri 1 23)
]

let links_output = lsp wait_result $client 10
$client = $links_output.session
let links = $links_output.result
assert (($links | length) == 1) "expected one document link"
let link_uri = $links.0.target
assert ($link_uri | str contains "/tags/dep/") "expected the document link target to use the tag path"

let locations_output = lsp wait_result $client 11
$client = $locations_output.session
let locations = $locations_output.result
assert (($locations | length) > 0) "expected a definition location"
let definition_uri = $locations.0.uri
assert ($definition_uri | str contains "/tags/dep/") "expected the definition URI to use the tag path"

let definition_path = lsp path_for_uri $definition_uri
assert ($definition_path | path exists) "expected the definition path to be materialized"

$client = lsp send_all $client [
	(lsp did_open $definition_uri (open $definition_path))
	(lsp hover 20 $definition_uri 0 16)
	(lsp diagnostics 21 $definition_uri)
]

let hover_output = lsp wait_response $client 20
$client = $hover_output.session
let diagnostics_output = lsp wait_result $client 21
$client = $diagnostics_output.session
let diagnostics = $diagnostics_output.result
assert (($diagnostics.items | length) == 0) "expected no diagnostics for the materialized definition"
lsp stop $client
