use ../../test.nu *
use ../lib/lsp.nu

let server = spawn

let path = artifact {
	tangram.ts: '
		const directory: tg.Directory = tg.directory();
		export default directory;
	'
}

let module_path = $path | path join "tangram.ts"
let module_uri = lsp uri $module_path
let source = open $module_path

let responses = lsp run [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $module_uri $source)
	(lsp definition 10 $module_uri 0 23)
]

let locations = lsp result $responses 10
assert (($locations | length) > 0) "expected a library definition location"
let definition_uri = $locations.0.uri
assert ($definition_uri | str contains "/tangram") "expected the definition URI to use the materialized library"

let definition_path = lsp path_for_uri $definition_uri
assert ($definition_path | path exists) "expected the library definition path to be materialized"

let definition_responses = lsp run [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $definition_uri (open $definition_path))
	(lsp diagnostics 20 $definition_uri)
]

let diagnostics = lsp result $definition_responses 20
assert (($diagnostics.items | length) == 0) "expected no diagnostics for the materialized library definition"
