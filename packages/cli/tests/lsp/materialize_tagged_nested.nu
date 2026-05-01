use ../../test.nu *
use ../lib/lsp.nu

let server = spawn

let dep_path = artifact {
	lib: {
		utils.tg.ts: '
			export const helper = () => "helper";
		'
	}
	tangram.ts: '
		export { helper } from "./lib/utils.tg.ts";
	'
}
tg tag nested $dep_path

let path = artifact {
	tangram.ts: '
		import { helper } from "nested";
		export default () => helper();
	'
}

let module_path = $path | path join "tangram.ts"
let module_uri = lsp uri $module_path
let source = open $module_path

let responses = lsp run [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $module_uri $source)
	(lsp document_link 10 $module_uri)
	(lsp definition 11 $module_uri 1 26)
]

let links = lsp result $responses 10
assert (($links | length) == 1) "expected one document link"
let link_uri = $links.0.target
assert ($link_uri | str contains "/tags/nested/tangram.ts") "expected the document link target to use the tag root path"

let locations = lsp result $responses 11
assert (($locations | length) > 0) "expected a definition location"
let definition_uri = $locations.0.uri
assert ($definition_uri | str contains "/tags/nested/lib/utils.tg.ts") "expected the definition URI to use the nested tag path"

let definition_path = lsp path_for_uri $definition_uri
assert ($definition_path | path exists) "expected the nested definition path to be materialized"

let definition_responses = lsp run [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $definition_uri (open $definition_path))
	(lsp hover 20 $definition_uri 0 16)
	(lsp diagnostics 21 $definition_uri)
]

lsp response $definition_responses 20 | ignore
let diagnostics = lsp result $definition_responses 21
assert (($diagnostics.items | length) == 0) "expected no diagnostics for the nested materialized definition"
