use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn --config {
	compiler: {
		check_backend: typescript7
		typescript_executable: /__missing_typescript7__/tsc
	}
}

let path = artifact {
	tangram.ts: 'export const value: number = "wrong";'
}

let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'failed to start the typescript 7 service')

let module_path = $path | path join tangram.ts
let module_uri = lsp uri $module_path
let responses = lsp exchange [
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $module_uri (open $module_path))
	(lsp diagnostics 10 $module_uri)
]
let diagnostics = lsp result $responses 10
assert (($diagnostics.items | length) > 0) 'expected TypeScript 6 LSP diagnostics'
