use ../../util/test.nu *
use ../../util/lsp.nu

# Editing the interface in the runtime package of a config module updates the types that its importers see.

let local = server spawn
let interface = '{ "exports": { "greet": { "type": { "kind": "command", "args": { "kind": "tuple", "elements": [] }, "output": { "kind": "OUTPUT" } } } } }'
let source = "import { greet } from \"./greet.tg.json\";\nexport default async () => { const text: string = await greet.build(); return text; };\n"
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	runtime: { config.tgi: ($interface | str replace OUTPUT string), tangram.ts: 'export default async () => null;' }
	tangram.ts: $source
}
let uri = lsp uri ($path | path join tangram.ts)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized) (lsp did_open $uri $source) (lsp diagnostics 10 $uri)]
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.items []

$interface | str replace OUTPUT number | save --force ($path | path join runtime/config.tgi)
$session = lsp send $session (lsp diagnostics 11 $uri)
let response = lsp wait_result $session 11
$session = $response.session
assert equal ($response.result.items | where severity == 1 | length) 1
lsp stop $session
