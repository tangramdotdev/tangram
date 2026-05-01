use ../../test.nu *
use ../lib/lsp.nu

let server_path = mktemp -d
let server = spawn --directory $server_path --config { vfs: {} }

if $nu.os-info.name == 'linux' {
	let mount_exit_code = do -i {
		^mountpoint -q ($server_path | path join "artifacts")
		$env.LAST_EXIT_CODE
	}
	assert ($mount_exit_code == 0) "expected the artifacts path to be mounted as a VFS"
}

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

let responses = lsp run [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $module_uri $source)
	(lsp definition 10 $module_uri 1 23)
]

let locations = lsp result $responses 10
assert (($locations | length) > 0) "expected a definition location"
let definition_uri = $locations.0.uri
assert ($definition_uri | str contains "/tags/dep/") "expected the definition URI to use the tag path"

let definition_path = lsp path_for_uri $definition_uri
assert ($definition_path | path exists) "expected the definition path to be materialized"

let definition_responses = lsp run [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $definition_uri (open $definition_path))
	(lsp hover 20 $definition_uri 0 16)
	(lsp diagnostics 21 $definition_uri)
]

lsp response $definition_responses 20 | ignore
let diagnostics = lsp result $definition_responses 21
assert (($diagnostics.items | length) == 0) "expected no diagnostics for the VFS materialized definition"
